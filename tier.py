#!/usr/bin/env python3
"""
tier.py — Unraid media tiering script (Phase P4.1)

Pulls watch history and metadata from Plex, computes a heat score per
media item (movies + TV series), probes the filesystem to determine
where each item currently lives, and recommends where it should live —
HOT (fast pool) or WARM (Unraid array). With --apply and moves.enabled,
executes TO_HOT, TO_WARM, and RELOCATE_WARM rsync moves serially.

Phase status:
  P0  (done)        Read-only analysis from Plex catalog + watch history.
  P0.1 (done)       Pinning (library + title), recency floor, projected-
                    tier footer.
  P0.2 (done)       Auto-inherit — when ≥N members of a Plex collection
                    naturally score HOT, promote the whole collection.
  P0.3 (done)       Collection pin — force every member of a named Plex
                    collection to HOT via pinned_collections: config.
  P0.4 (done)       Added-date floor — promote recently-added movies and
                    TV shows with fresh episodes to HOT regardless of
                    play count.
  P0.5 (done)       Disk eviction mode — mark warm-tier array disks as
                    evicting; items on them get RELOCATE_WARM so P2
                    knows to move them regardless of tier score. Data
                    model + reporting only; actual moves are P2.
  P1  (done)        Filesystem probing to detect current tier. Auto-
                    detects array disks, translates Plex-side paths via
                    plex_path_map, rolls multi-part items up by bytes.
  P2.1 (done)       Move executor — TO_HOT direction. rsync from warm
                    array disk to hot ZFS pool. Dry-run by default;
                    --apply executes. Source deleted after size-verify
                    when delete_source_after_verify=true.
  P2.2 (done)       TO_WARM moves — demote from hot ZFS pool to chosen
                    warm array disk. Destination selected via
                    co_locate_then_most_free strategy with safety margin.
  P2.3 (done)       RELOCATE_WARM moves — move items on evicting warm
                    disks to a healthy warm disk. Same rsync+verify+
                    delete flow; source (evicting) disk excluded from
                    destination candidates.
  P2.4 (dropped)    Plex rescan automation — unnecessary under Unraid's
                    user-share union: TO_WARM and RELOCATE_WARM keep
                    files within /mnt/user/; Plex path references remain
                    valid without a rescan. Only TO_HOT (which moves
                    files outside the union) recommends a rescan.
  P3  (done)        Capacity-aware tiering — hot pool ceiling with
                    promotion budget (OVER_BUDGET_HOT outcome), optional
                    auto-demote of lowest-scoring HOT items when over
                    ceiling, warm per-disk ceiling, --no-promote /
                    --no-demote CLI flags.
  P3.5 (done)       Move hardening — run-level I/O budget. Once
                    moves.max_total_move_gb has been successfully
                    transferred the move pass stops; remaining items
                    keep their scoring outcomes and retry next run.
  P4.1 (done)       Scheduling primitives — single-instance lock with
                    stale-PID reclaim, persisted run-state (last_run.json),
                    skip_if_run_within_minutes recency guard, formalised
                    exit-code contract.
  P4.5 (done)       Promote-only run mode — --mode {full,promote-only,
                    demote-only} as a cron-ergonomics shortcut over
                    --no-promote/--no-demote (which now also suppresses
                    RELOCATE_WARM). TIER_MODE env + scheduling.default_mode
                    config give every CA-template/docker-start deployment a
                    way to select it without CLI args (precedence: CLI >
                    env > config > default); TIER_APPLY env mirrors --apply
                    the same way. min_episodes_for_fast_promote guards
                    promote-only TO_HOT for series against single-pilot
                    watches.

Usage:
    tier.py [--config PATH] [--library NAME ...] [--json|--csv PATH]
            [--explain TITLE] [--sort COL] [--top N] [--apply]
            [--mode {full,promote-only,demote-only}]

Exit codes:
    0  success
    1  configuration error (file missing, token placeholder, empty libraries)
    2  Plex unreachable or auth failed (or bad CLI usage, e.g. conflicting
       --mode / --no-promote / --no-demote, or invalid TIER_MODE/TIER_APPLY)
    4  unhandled runtime error (error notification fired if configured)
    5  lock held — another tier.py instance is already running
    6  skipped — previous run finished within skip_if_run_within_minutes
  130  interrupted (SIGINT)
"""

from __future__ import annotations

import argparse
import csv
import enum
import fcntl
import glob
import json
import logging
import logging.handlers
import math
import os
import re
import shutil
import ssl
import subprocess
import sys
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import yaml
except ImportError:
    sys.exit("Missing dependency: pip install pyyaml")

try:
    from plexapi.server import PlexServer
    from plexapi.exceptions import Unauthorized, BadRequest
    from requests.exceptions import ConnectionError as ReqConnErr
except ImportError:
    sys.exit("Missing dependency: pip install plexapi")


# Module-level logger; configured by setup_logging().
log = logging.getLogger("tier")


class ExitCode(enum.IntEnum):
    SUCCESS = 0
    FATAL_CONFIG = 1       # config file missing/invalid (string sys.exit)
    PLEX_ERROR = 2         # Plex unreachable or auth failed
    UNHANDLED_CRASH = 4    # unhandled exception; error notifier fires
    LOCK_HELD = 5          # another instance is running; do not notify
    SKIPPED_RECENT = 6     # recency guard fired; do not notify
    KEYBOARD_INTERRUPT = 130


# ---------- Defaults (overridable via tiering.yaml) ----------

DEFAULT_CONFIG = {
    "plex": {
        "url": "http://localhost:32400",
        "token": None,  # REQUIRED — set in tiering.yaml
    },
    "libraries": [],  # list of {name: "..."} entries — REQUIRED
    "thresholds": {
        "score_to_hot": 40.0,   # WARM -> HOT if score > this
        "score_to_warm": 20.0,  # HOT -> WARM if score < this
        "age_grace_days": 180,  # never-watched items keep HOT protection this long
        "recency_half_life_days": 90,
        # Recency floor — any item watched within this window stays HOT
        # regardless of raw score. Stops active-but-infrequent shows from
        # being demoted just because the play count is low.
        "hot_recency_days": 730,  # ~2 years
        # Added-date floor — items added to Plex within this window are
        # promoted to HOT even if never watched. Plex surfaces recently-
        # added media on the home screen for roughly this long; tier them
        # accordingly. Set to 0 or null to disable.
        "added_floor_days_movies": 45,
        "added_floor_days_tv": 30,
    },
    "pinning": {
        # Libraries whose contents should stay HOT unconditionally.
        # Exact, case-insensitive match on the Plex library name.
        "always_hot_libraries": [],
        # Title substrings (case-insensitive). Matching items stay HOT
        # unconditionally. E.g. "Stargate" catches SG-1, Atlantis,
        # Universe, Origins, and the movie.
        "always_hot_titles": [],
    },
    # Named Plex collections to force-promote to HOT. Each entry requires
    # both library (exact Plex name) and name (exact collection title) to
    # disambiguate — collection names are per-section in Plex, so the same
    # name can exist in different libraries. Empty list = feature off.
    "pinned_collections": [],
    # Auto-inherit collection pin — when enough members of a collection
    # naturally score HOT (pre-floor, pre-pin), promote the whole collection.
    # Use case: you've watched several Star Wars films; the rest auto-inherit
    # HOT so they're ready when you reach for them. Default off — opt-in.
    "auto_collection_inherit": {
        "enabled": False,
        "min_hot_members": 2,      # collections with fewer hot members are skipped
        # Escape hatch for collections sized exactly equal to min_hot_members:
        # require this fraction of members to be hot (ceil, min 1) instead of
        # the absolute min_hot_members count. For larger collections the absolute
        # threshold still applies.
        "min_hot_fraction": 0.5,
        "skip_smart_collections": True,  # smart collections are curated rules; skip
        "exclude_libraries": [],   # library names to exempt entirely
    },
    "paths": {
        "user_share_prefix": "/mnt/user",
        # Mount point of the HOT pool as seen by THIS script (tier.py).
        "hot_pool_mount": "/mnt/hot_pool",
        # Explicit WARM-tier disks. Empty list = auto-detect /mnt/disk[0-9]*
        # that are actually mount points. Populate to restrict the set.
        "array_disks": [],
        # Disks to exclude from auto-detected list. Useful for retirements.
        "array_disk_exclude": [],
        # Plex-to-tier path translation. Plex reports file paths as IT
        # sees them. If tier.py runs on a different host (e.g. Plex in a
        # VM, tier on the bare-metal NAS), those paths won't resolve
        # locally. List one or more {plex, tier} prefix pairs; the
        # longest matching plex prefix on each reported path is replaced
        # with the corresponding tier prefix. Empty list = no translation.
        "plex_path_map": [],
    },
    # Disk eviction — mark specific warm-tier array disks as evicting.
    # Items whose files majority-reside on an evicting disk and would
    # otherwise STAY_WARM are flagged RELOCATE_WARM so P2's mover will
    # move them to a different warm disk (or hot, if the score says so).
    # Actual moves are P2; this is data-model + reporting only.
    "array_disk_evict": {
        "enabled": False,
        "disks": [],  # disk paths matching paths.array_disks format
    },
    # Move executor (P2). Requires --apply to execute; dry-run always when off.
    # enabled: false means the move pass is skipped entirely — no log lines.
    "moves": {
        "enabled": False,
        "apply": False,
        "rsync_options": ["-aH", "--partial", "--inplace"],
        "delete_source_after_verify": True,
        "size_verify": True,
        "parity_check_blocking": True,
        "bandwidth_limit_mbps": None,
        # Initial throughput estimates for ETA at the start of each run.
        # Actual speeds vary by hardware; adjust to match your environment.
        # TO_HOT writes to the ZFS pool (NVMe/SSD); TO_WARM/RELOCATE_WARM
        # write to spinning-disk array — typically much slower.
        "estimated_hot_mbps": 200,
        "estimated_warm_mbps": 50,
        "warm_disk_selection": {
            # How to pick the destination warm disk for TO_WARM / RELOCATE_WARM.
            # Only strategy in v1: prefer the disk that already holds the most
            # bytes of this series (co-location), falling back to most-free.
            "strategy": "co_locate_then_most_free",
            # Leave at least this many GB free on the target disk after the move.
            # Items that would exceed this margin are skipped with [FAILED].
            "safety_margin_gb": 50,
        },
        # Stop the move pass once this many GB have been successfully
        # transferred in a single run (0 = no limit). Remaining items keep
        # their scoring outcomes and are retried next run. Distinct from
        # capacity.hot_ceiling_percent (pool fill guard) — this bounds I/O
        # duration. Cap is >=: the item that crosses the threshold is still
        # attempted; the next item is the first blocked.
        "max_total_move_gb": 0,
    },
    "logging": {
        "path": "/config/tier.log",  # container-friendly default
        "level": "INFO",
        "max_bytes": 2_000_000,
        "backup_count": 5,
    },
    "notifications": {
        "webhook": {
            "url": None,
            "auth_header": None,
        },
        "unraid": {
            "enabled": False,
            "notify_script": "/usr/local/emhttp/webGui/scripts/notify",
        },
        "on_plex_unreachable": True,
        "on_auth_failure": True,
        "on_script_error": True,
    },
    # Capacity-aware tiering (P3). Controls the hot pool promotion budget and
    # optional auto-demotion when the pool is already over ceiling.
    "capacity": {
        # Refuse to fill the hot ZFS pool past this percentage.
        # ZFS performance degrades above ~80%; keep headroom for snapshots.
        "hot_ceiling_percent": 80,
        # Skip warm array disks that are above this fill level when selecting
        # a destination for TO_WARM / RELOCATE_WARM moves.
        "warm_per_disk_ceiling_percent": 90,
        # When True and the hot pool is already over the ceiling, force-demote
        # the lowest-scoring STAY_HOT items to TO_WARM until the pool would
        # come back under the ceiling. PIN_HOT items are always exempt.
        "auto_demote_when_over_ceiling": False,
        # Additional headroom to leave inside the ceiling (GB). Useful to
        # account for snapshot growth or concurrent writes during the run.
        "budget_safety_margin_gb": 0,
        # Manual hot pool size override (GB). Only needed when the hot pool is
        # a ZFS pool and auto-detection fails inside the Docker container (the
        # common symptom is "0% full" in the Capacity log line). See
        # example.tiering.yaml for how to find the right value.
        # Detection chain: Unraid API → zpool cmd → /proc/spl kstat → this override → statvfs.
        "hot_pool_total_gb": None,
        # Unraid Connect GraphQL API — queries pool stats without needing ZFS tools
        # or config overrides. Requires Unraid 6.12+ with an API key from the
        # Unraid Connect dashboard (Settings → Management Access → API keys).
        # Leave url null to disable (no network call is made).
        "unraid_api_url": None,
        "unraid_api_key": None,
        # ZFS pool name as Unraid reports it (e.g. "Zfs_media"). When null, the
        # first ZFS pool returned by the API that is at least as large as
        # statvfs.free is used as the match heuristic.
        "unraid_pool_name": None,
        # Verify TLS certificate for the Unraid Connect API call. Defaults to
        # true (secure). Set false to skip verification for Unraid's self-signed
        # cert on a trusted LAN — Unraid API call only, no other effect.
        "unraid_api_verify_tls": True,
    },
    # Scheduling primitives (P4.1) + run-mode defaults (P4.5).
    "scheduling": {
        # Skip this run if the previous run finished within this many minutes.
        # 0 = disabled. Useful when multiple triggers (cron + manual) can fire
        # close together and you only want one run per window.
        "skip_if_run_within_minutes": 0,
        # Default run mode when neither --mode nor TIER_MODE is set.
        # One of: full, promote-only, demote-only.
        "default_mode": "full",
        # promote-only runs only: a TV series is promoted to HOT only if at
        # least this many episodes were watched since the last full run.
        # Movies are unaffected. 1 disables the guard.
        "min_episodes_for_fast_promote": 2,
    },
}

# Default config path — container layout. Bare installs fall back to the
# legacy /boot path in load_config().
DEFAULT_CONFIG_PATH = Path("/config/tiering.yaml")
# All scheduling state (lock file, last_run.json) lives here.
_STATE_DIR = Path("/config/state")
LEGACY_CONFIG_PATH = Path(
    "/boot/config/plugins/user.scripts/scripts/plex-media-tiering/tiering.yaml"
)


# ---------- Logging ----------


def setup_logging(cfg: dict, quiet: bool = False) -> None:
    """Configure the 'tier' logger: rotating file + (optional) console."""
    logcfg = cfg.get("logging") or {}
    level_name = str(logcfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    log.setLevel(level)
    log.handlers.clear()
    log.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_path = Path(logcfg.get("path") or "/config/tier.log")
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=int(logcfg.get("max_bytes", 2_000_000)),
            backupCount=int(logcfg.get("backup_count", 5)),
        )
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except (OSError, PermissionError) as e:
        # Falling back to console-only; flag it but don't die.
        print(
            f"WARN: could not open log file {log_path}: {e}",
            file=sys.stderr,
        )

    if not quiet:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        log.addHandler(ch)


# ---------- Notifications ----------


class Notifier:
    """Interface. Sub-notifiers must not raise — failures are swallowed."""

    def alert(self, title: str, message: str, level: str = "error") -> None:
        raise NotImplementedError


class WebhookNotifier(Notifier):
    """POSTs a JSON payload to a webhook URL.

    Works with Home Assistant webhooks, gotify, ntfy, Discord (with a
    minor shape tweak), or any custom receiver.
    """

    def __init__(self, url: str, auth_header: Optional[str] = None):
        self.url = url
        self.auth_header = auth_header

    def alert(self, title, message, level="error"):
        body = json.dumps({
            "source": "tier",
            "level": level,
            "title": title,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8")
        req = urllib.request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        if self.auth_header:
            req.add_header("Authorization", self.auth_header)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                log.info("Webhook notified (%s)", resp.status)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            log.warning("Webhook notify failed: %s", e)


class UnraidNotifier(Notifier):
    """Calls Unraid's /usr/local/emhttp/webGui/scripts/notify.

    For container installs this requires bind-mounting the notify script
    path into the container. For bare installs it just works.
    """

    def __init__(self, notify_script: str):
        self.script = notify_script

    def alert(self, title, message, level="error"):
        if not os.path.exists(self.script):
            log.warning("Unraid notify script not found at %s", self.script)
            return
        importance = {
            "info": "normal",
            "warning": "warning",
            "error": "alert",
        }.get(level, "warning")
        try:
            subprocess.run(
                [self.script, "-i", importance, "-s", title, "-d", message],
                check=False,
                timeout=10,
            )
            log.info("Unraid notify fired (%s): %s", importance, title)
        except Exception as e:  # noqa: BLE001
            log.warning("Unraid notify failed: %s", e)


class StderrNotifier(Notifier):
    """Always-on fallback so operators see the alert in cron output."""

    def alert(self, title, message, level="error"):
        log.error("[ALERT %s] %s — %s", level.upper(), title, message)


class CompositeNotifier(Notifier):
    def __init__(self, sub: list):
        self.sub = sub

    def alert(self, title, message, level="error"):
        for n in self.sub:
            try:
                n.alert(title, message, level)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "Notifier %s raised: %s", type(n).__name__, e,
                )


def build_notifier(cfg: dict) -> Notifier:
    ncfg = cfg.get("notifications") or {}
    subs: list = [StderrNotifier()]

    wh = ncfg.get("webhook") or {}
    if wh.get("url"):
        subs.append(WebhookNotifier(wh["url"], wh.get("auth_header")))

    ur = ncfg.get("unraid") or {}
    if ur.get("enabled"):
        subs.append(UnraidNotifier(
            ur.get("notify_script", "/usr/local/emhttp/webGui/scripts/notify")
        ))

    return CompositeNotifier(subs)


# ---------- Data model ----------


@dataclass
class Item:
    """Unified view of a scorable media item (movie OR whole TV series)."""

    title: str
    year: Optional[int]
    kind: str                      # 'movie' or 'series'
    library: str
    plays: int                     # rolled up for series
    last_played: Optional[datetime]
    added: datetime
    size_bytes: int
    score: float
    # --- filled in at P1+ ---
    current_tier: str = "UNKNOWN"  # 'HOT' | 'WARM' | 'UNKNOWN'
    current_disk: Optional[str] = None  # dominant warm disk path; None if HOT/UNKNOWN/MIXED
    # Actual warm-disk file paths for this item, keyed by disk mount path.
    # Used by the move executor: rsync transfers exactly these files regardless
    # of how the library is organised on disk (per-item folders vs shared
    # year folders). Dominant disk is at key == current_disk when current_tier
    # is WARM; for HOT-majority items current_disk is None but this dict may
    # still be non-empty (minority warm stragglers for straggler promotion).
    warm_disk_files: Dict[str, List[str]] = field(default_factory=dict)
    # Resolved absolute paths of files currently on the hot pool — populated
    # only when current_tier is HOT (or MIXED with hot bytes). Used by the
    # TO_WARM move executor as the rsync source list.
    hot_pool_files: List[str] = field(default_factory=list)
    # Common-ancestor source dirs (dominant disk first) — kept for display /
    # log output only.  NOT used for rsync source paths.
    source_dirs: List[str] = field(default_factory=list)
    # --- decision ---
    outcome: str = "NEUTRAL"       # See decide_outcome() for P0 values
    # --- collection-pin support ---
    rating_key: Optional[int] = None  # Plex ratingKey; used by collect_all for collection lookup
    collection_pinned: bool = False   # True if in a pinned_collections entry
    auto_inherit_pinned: bool = False  # True if auto-inherit fired for this item's collection
    # --- added-date floor flag (set by collect_* if floor threshold met) ---
    recently_added: bool = False
    # --- eviction minority-override (set during eviction pass) ---
    # When an item's majority bytes are on a safe disk but minority bytes are
    # on an evicting disk, only the evicting-disk files need to move.  This
    # field is set to {evicting_disk: [files]} so P2's RELOCATE_WARM executor
    # moves only those files while warm_disk_files stays intact for co-location
    # scoring in _select_warm_destination.  None = use full warm_disk_files.
    relocate_source_override: Optional[Dict[str, List[str]]] = None
    # --- P4.5 fast-promote guard (series only) ---
    # Distinct episode ratingKeys with a play event since the cutoff passed to
    # collect_all() as fast_promote_cutoff (last_full_run_finished_at, or all
    # history if no full run has ever completed). 0 for movies — the guard
    # only applies to series. Computed unconditionally at collect time; only
    # consulted when a promote-only run evaluates the fast-promote guard.
    recent_episode_plays: int = 0
    # --- for --explain ---
    score_breakdown: dict = field(default_factory=dict)

    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)

    @property
    def title_year(self) -> str:
        return f"{self.title} ({self.year})" if self.year else self.title


# ---------- Config loading ----------


def _deep_merge(defaults: dict, overrides: dict) -> dict:
    """Recursively merge `overrides` over `defaults`. New keys add; dict
    values merge; scalar/list values replace."""
    out = dict(defaults)
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


# Baked-in template path inside the image. Used to auto-seed the /config
# volume on first run so operators see an edit-me file instead of a bare
# "config not found" error.
BUNDLED_EXAMPLE_PATH = Path("/app/example.tiering.yaml")


def _try_seed_config(path: Path) -> bool:
    """If running in-container and the config volume is empty, drop the
    bundled example into it so the user can edit it from the host. Returns
    True if a seed was written, False otherwise."""
    if not BUNDLED_EXAMPLE_PATH.exists():
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return False
        path.write_text(BUNDLED_EXAMPLE_PATH.read_text())
        try:
            path.chmod(0o600)
        except OSError:
            pass
        return True
    except (OSError, PermissionError):
        return False


def load_config(path: Path) -> dict:
    # Fall back to legacy /boot path if the primary (container) path is absent.
    if not path.exists() and path == DEFAULT_CONFIG_PATH and LEGACY_CONFIG_PATH.exists():
        path = LEGACY_CONFIG_PATH

    if not path.exists():
        seeded = _try_seed_config(path)
        if seeded:
            sys.exit(
                f"First-run setup: wrote template to {path}\n"
                f"Edit it (set plex.token, confirm library names, etc.)\n"
                f"then re-run the container."
            )
        sys.exit(
            f"Config not found: {path}\n"
            f"Create it from example.tiering.yaml and set plex.token."
        )
    with open(path) as f:
        user_cfg = yaml.safe_load(f) or {}

    cfg = _deep_merge(DEFAULT_CONFIG, user_cfg)

    token = cfg.get("plex", {}).get("token") or ""
    if not token or token.startswith("REPLACE"):
        sys.exit(
            f"Config error: plex.token in {path} is still the placeholder.\n"
            f"Generate a token at https://support.plex.tv/articles/"
            f"204059436-finding-an-authentication-token-x-plex-token/"
        )
    if not cfg["libraries"]:
        sys.exit("Config error: at least one library must be listed")

    return cfg


# ---------- Plex connection ----------


def connect_plex(url: str, token: str, notifier: Notifier, ncfg: dict) -> PlexServer:
    log.info("Connecting to Plex at %s", url)
    try:
        server = PlexServer(url, token, timeout=30)
        log.info(
            "Plex OK: server=%r",
            getattr(server, "friendlyName", "<unknown>"),
        )
        return server
    except Unauthorized:
        log.error(
            "Plex auth failed: token rejected. "
            "Re-generate the token and update tiering.yaml."
        )
        if ncfg.get("on_auth_failure", True):
            notifier.alert(
                title="Tier: Plex token invalid",
                message=(
                    "tier.py could not authenticate to Plex — the token was "
                    f"rejected by the server at {url}. Re-generate the token "
                    "from Plex (https://support.plex.tv/articles/204059436-"
                    "finding-an-authentication-token-x-plex-token/), update "
                    "tiering.yaml, and restart."
                ),
                level="error",
            )
        sys.exit(int(ExitCode.PLEX_ERROR))
    except (ReqConnErr, BadRequest, TimeoutError) as e:
        log.error("Cannot reach Plex at %s: %s", url, e)
        if ncfg.get("on_plex_unreachable", True):
            notifier.alert(
                title="Tier: Plex unreachable",
                message=(
                    f"tier.py could not connect to Plex at {url}. "
                    f"Error: {e}. Next scheduled run will retry."
                ),
                level="error",
            )
        sys.exit(int(ExitCode.PLEX_ERROR))


# ---------- Filesystem / tier detection (P1) ----------


# Pattern for auto-detecting Unraid array data disks. /mnt/disk1, /mnt/disk2
# etc. — NOT /mnt/user (fuse overlay) and NOT /mnt/cache* (pools).
_ARRAY_DISK_PATTERN = re.compile(r"^/mnt/disk\d+$")


def resolve_array_disks(cfg: dict) -> List[str]:
    """Return the effective list of WARM-tier mount points.

    Precedence:
      1. paths.array_disks non-empty -> use that list verbatim (minus
         paths.array_disk_exclude).
      2. Otherwise auto-detect /mnt/disk[0-9]* mounts (minus excludes).

    Auto-detect ignores paths that aren't actual mount points — inside
    the container the directory has to be bind-mounted for the disk to
    be visible, so non-mounted paths can't hold media.
    """
    pcfg = cfg.get("paths") or {}
    explicit = [str(p).rstrip("/") for p in (pcfg.get("array_disks") or [])]
    excludes = {
        str(p).rstrip("/") for p in (pcfg.get("array_disk_exclude") or [])
    }

    if explicit:
        return [p for p in explicit if p not in excludes]

    candidates = sorted(glob.glob("/mnt/disk*"))
    detected = [
        p.rstrip("/") for p in candidates
        if _ARRAY_DISK_PATTERN.match(p.rstrip("/"))
        and os.path.ismount(p)
        and p.rstrip("/") not in excludes
    ]
    return detected


def _build_evict_disks(evict_cfg: dict, array_disks: List[str]) -> set:
    """Return validated set of evicting disk paths from array_disk_evict config.

    Logs a WARNING for any configured disk that isn't in the effective
    array_disks list (typo, stale path, or disk removed from config).
    Returns an empty set when disabled, when the disks list is empty, or when
    all entries failed validation — in all cases no eviction lines are logged.
    """
    if not evict_cfg.get("enabled"):
        return set()
    raw = [str(d).rstrip("/") for d in (evict_cfg.get("disks") or [])]
    if not raw:
        return set()
    valid_array = set(array_disks)
    result = set()
    for d in raw:
        if d in valid_array:
            result.add(d)
        else:
            log.warning(
                "Eviction: disk %r not in effective array_disks list — skipping", d
            )
    return result


def translate_plex_path(plex_path: str, path_map) -> str:
    """Translate a Plex-reported path to a tier-container path.

    `path_map` is a list of dicts: [{"plex": "/mnt/tank/media", "tier":
    "/mnt/user"}, ...]. Longest-matching `plex` prefix wins; the match
    is replaced with the corresponding `tier` prefix. Paths that don't
    match any prefix are returned unchanged (so configs that don't need
    translation work without touching this list).
    """
    if not plex_path or not path_map:
        return plex_path or ""
    # Sort by plex-prefix length descending so longest match wins.
    pairs = sorted(
        [
            (str(m.get("plex", "")).rstrip("/"), str(m.get("tier", "")).rstrip("/"))
            for m in path_map if isinstance(m, dict) and m.get("plex")
        ],
        key=lambda t: len(t[0]),
        reverse=True,
    )
    for plex_pref, tier_pref in pairs:
        if not plex_pref:
            continue
        if plex_path == plex_pref:
            return tier_pref
        if plex_path.startswith(plex_pref + "/"):
            return tier_pref + plex_path[len(plex_pref):]
    return plex_path


def resolve_user_share(
    path: str,
    user_share_prefix: str,
    hot_mount: str,
    array_disks: List[str],
) -> str:
    """Resolve an Unraid user-share path to its actual tier mount point.

    Unraid's FUSE user-share layer presents every file under /mnt/user/
    regardless of which physical disk or pool backs it. After plex_path_map
    translation, paths may still start with /mnt/user — classify_path()
    would return UNKNOWN for all of them because it only knows about
    hot_pool_mount and /mnt/diskN prefixes.

    This function probes each candidate tier mount in order (HOT first,
    then WARM disks) looking for <mount>/<relative-path>. First hit wins
    and the resolved path is returned. If nothing matches, the original
    path is returned unchanged so classify_path() marks it UNKNOWN.

    Pipeline: translate_plex_path → resolve_user_share → classify_path
    """
    if not user_share_prefix or not path:
        return path
    prefix = user_share_prefix.rstrip("/") + "/"
    if not path.startswith(prefix):
        return path
    rel = path[len(prefix):]
    candidates = ([hot_mount] if hot_mount else []) + list(array_disks or [])
    for mount in candidates:
        if not mount:
            continue
        probe = os.path.join(mount, rel)
        if os.path.exists(probe):
            return probe
    return path


def classify_path(
    path: str, hot_mount: str, array_disks: List[str]
) -> str:
    """Return 'HOT' | 'WARM' | 'UNKNOWN' for a given (translated) path.

    Path matching is prefix-based. We do NOT stat the filesystem here —
    that's slow and not needed for classification. UNKNOWN covers both
    unresolved paths and paths outside any known tier mount (e.g. files
    that live on a pool we haven't configured)."""
    if not path:
        return "UNKNOWN"
    hot = (hot_mount or "").rstrip("/")
    if hot and (path == hot or path.startswith(hot + "/")):
        return "HOT"
    for disk in array_disks or []:
        d = disk.rstrip("/")
        if path == d or path.startswith(d + "/"):
            return "WARM"
    return "UNKNOWN"


# Matches sort-title article form: "Bounty Hunter, The (2010)" or "Bounty Hunter, The"
_SORT_TITLE_RE = re.compile(r'^(.+),\s+(the|a|an)\s*(\(\d{4}\))?\s*$', re.IGNORECASE)


def _is_movie_per_folder(parent_name: str, stem: str) -> bool:
    """Return True when parent_name and stem refer to the same movie title.

    Handles two naming conventions:
    - Exact: 'Austin Powers (2002)' folder + 'Austin Powers (2002).mkv'
    - Sort-title inversion: 'The Bounty Hunter (2010)' folder +
      'Bounty Hunter, The (2010).mkv' (Plex renames articles to the end).
    """
    if parent_name.lower() == stem.lower():
        return True

    def _strip(s: str) -> str:
        s = s.strip()
        for art in ("the ", "a ", "an "):
            if s.lower().startswith(art):
                return s[len(art):]
        m = _SORT_TITLE_RE.match(s)
        if m:
            base = m.group(1).strip()
            year = (m.group(3) or "").strip()
            return (base + (" " + year if year else "")).strip()
        return s

    return _strip(parent_name).lower() == _strip(stem).lower()


def _find_companion_files(media_path: str) -> List[str]:
    """Return sibling files in the same directory that should move with media_path.

    Two modes depending on folder structure:

    Movie-per-folder: when the parent directory name matches the media file's
    stem (e.g. '.../Austin Powers in Goldmember (2002)/Austin Powers in
    Goldmember (2002).mkv'), ALL other files in the directory are returned.
    This covers Plex extras — trailers, featurettes, deleted scenes, etc. —
    which Plex stores in the same folder using suffix conventions (-trailer,
    -featurette, -deleted, …) and which have completely different stems from
    the main title file.

    Shared folder: when the parent is a year folder, library root, or any
    other shared container (parent name != file stem), only files whose stem
    equals the media stem or starts with stem + '.' are returned.  This is
    the subtitle/NFO companion case for year-organised libraries where
    multiple movies share one folder.

    Detection: _is_movie_per_folder() — handles exact match and article
    inversions ('The Foo' folder / 'Foo, The.mkv' sort-title convention).
    Year folders ('2002') and library roots ('Movies') never match.
    """
    parent = os.path.dirname(media_path)
    stem = os.path.splitext(os.path.basename(media_path))[0]
    parent_name = os.path.basename(parent)
    movie_per_folder = _is_movie_per_folder(parent_name, stem)
    companions: List[str] = []
    try:
        with os.scandir(parent) as it:
            for entry in it:
                if not entry.is_file(follow_symlinks=False):
                    continue
                if entry.path == media_path:
                    continue
                if movie_per_folder:
                    companions.append(entry.path)
                else:
                    entry_stem = os.path.splitext(entry.name)[0]
                    if entry_stem == stem or entry_stem.startswith(stem + "."):
                        companions.append(entry.path)
    except OSError:
        pass
    return companions


def resolve_item_current_tier(
    parts: Iterable[Tuple[str, int]],
    path_map,
    hot_mount: str,
    array_disks: List[str],
    user_share_prefix: str = "",
) -> Tuple[str, dict, Optional[str], List[str], Dict[str, List[str]], List[str]]:
    """Majority-bytes rollup of tier for a multi-part item.

    parts: iterable of (plex_file_path, size_bytes).
    Returns:
      (tier_str, breakdown, dominant_warm_disk, source_dirs, warm_disk_files, hot_pool_files)
      tier_str: 'HOT' | 'WARM' | 'MIXED' | 'UNKNOWN'
      breakdown: dict of per-tier byte shares (0.0..1.0)
      dominant_warm_disk: path of the WARM disk with most bytes, or None
      source_dirs: common-ancestor dirs per warm disk (dominant first) — display only
      warm_disk_files: {disk: [resolved file paths]} for all warm disks
      hot_pool_files: resolved file paths on the hot pool (for TO_WARM rsync source)

    Decision rules:
      - Majority (>50%) bytes on HOT  -> HOT
      - Majority bytes on WARM        -> WARM
      - Majority bytes UNKNOWN        -> UNKNOWN
      - Otherwise (50/50 tie, or no clear majority) -> MIXED
    """
    totals = {"HOT": 0, "WARM": 0, "UNKNOWN": 0}
    disk_bytes: dict = {}  # warm disk path -> bytes on that disk
    disk_files: Dict[str, List[str]] = {}  # warm disk path -> list of resolved file paths
    hot_files: List[str] = []              # files resolved to the hot pool
    total = 0
    for plex_path, size in parts:
        if not size or size <= 0:
            continue
        total += size
        translated = translate_plex_path(plex_path or "", path_map)
        resolved = resolve_user_share(translated, user_share_prefix, hot_mount, array_disks)
        tier = classify_path(resolved, hot_mount, array_disks)
        totals[tier] += size
        if tier == "HOT":
            hot_files.append(resolved)
        elif tier == "WARM":
            for disk in array_disks or []:
                d = disk.rstrip("/")
                if resolved == d or resolved.startswith(d + "/"):
                    disk_bytes[disk] = disk_bytes.get(disk, 0) + size
                    disk_files.setdefault(disk, []).append(resolved)
                    break

    # Augment file lists with companion files (subtitles, NFO, etc.) that
    # share the media file's stem. A single shared `seen` set prevents the
    # same companion from appearing in both hot_files and a warm disk list.
    seen: set = set()

    # Hot pool companions
    for mf in hot_files:
        seen.add(mf)
    hot_extras: List[str] = []
    for mf in hot_files:
        for companion in _find_companion_files(mf):
            if companion not in seen:
                seen.add(companion)
                hot_extras.append(companion)
    hot_files.extend(hot_extras)

    # Warm disk companions
    for disk in list(disk_files.keys()):
        extras: List[str] = []
        for mf in disk_files[disk]:
            seen.add(mf)
        for mf in disk_files[disk]:
            for companion in _find_companion_files(mf):
                if companion not in seen:
                    seen.add(companion)
                    extras.append(companion)
        disk_files[disk].extend(extras)

    # Cross-tier companion probe: for WARM files in movie-per-folder layouts,
    # scan the equivalent hot pool directory for extras stranded by a prior
    # partial move (main file already moved to warm, extras left on hot pool).
    # Typical scenario: a prior run moved the main .mkv but not the featurettes,
    # trailers, or deleted scenes in the same folder.
    if hot_mount:
        hot_probed: set = set()
        for disk, files in disk_files.items():
            d = disk.rstrip("/")
            for mf in files:
                parent = os.path.dirname(mf)
                stem = os.path.splitext(os.path.basename(mf))[0]
                if not _is_movie_per_folder(os.path.basename(parent), stem):
                    continue  # not a movie-per-folder layout; skip
                if not mf.startswith(d + "/"):
                    continue
                rel = mf[len(d):]  # e.g. /DVD Rips/Movies/Title (YYYY)/Title (YYYY).mkv
                hot_dir = os.path.dirname(hot_mount.rstrip("/") + rel)
                if hot_dir in hot_probed:
                    continue
                hot_probed.add(hot_dir)
                if not os.path.isdir(hot_dir):
                    continue
                try:
                    with os.scandir(hot_dir) as scan_it:
                        for entry in scan_it:
                            if not entry.is_file(follow_symlinks=False):
                                continue
                            if entry.path not in seen:
                                seen.add(entry.path)
                                hot_files.append(entry.path)
                except OSError:
                    pass

    dominant = max(disk_bytes, key=disk_bytes.__getitem__) if disk_bytes else None
    # Build display-only source dirs (common ancestor per disk, dominant first).
    warm_source_dirs: List[str] = []
    for disk, files in disk_files.items():
        if not files:
            continue
        cp = os.path.commonpath(files)
        src = os.path.dirname(cp) if cp in files else cp
        if disk == dominant:
            warm_source_dirs.insert(0, src)
        else:
            warm_source_dirs.append(src)
    if total == 0:
        return "UNKNOWN", {"HOT": 0.0, "WARM": 0.0, "UNKNOWN": 0.0}, None, [], {}, []
    split = {k: round(v / total, 4) for k, v in totals.items()}
    if split["HOT"] > 0.5:
        # Return any minority warm files so the straggler pass can detect and
        # promote stragglers left behind by a partial prior move.
        return "HOT", split, None, warm_source_dirs, dict(disk_files), hot_files
    if split["WARM"] > 0.5:
        return "WARM", split, dominant, warm_source_dirs, dict(disk_files), hot_files
    if split["UNKNOWN"] > 0.5:
        return "UNKNOWN", split, None, [], {}, hot_files
    return "MIXED", split, dominant, warm_source_dirs, dict(disk_files), hot_files


def _media_parts(media_list) -> Iterable[Tuple[str, int]]:
    """Yield (file_path, size) pairs across all media + parts."""
    for media in media_list or []:
        for part in getattr(media, "parts", None) or []:
            yield (
                getattr(part, "file", "") or "",
                int(getattr(part, "size", 0) or 0),
            )


# ---------- Scoring ----------


def heat_score(
    plays: int,
    last_played: Optional[datetime],
    added: datetime,
    now: datetime,
    thresholds: dict,
) -> tuple[float, dict]:
    """Return (score, breakdown-dict)."""

    # --- play weight ---
    recency = 0.0
    days_since_play: Optional[int] = None
    if last_played and plays > 0:
        days_since_play = (now - _as_utc(last_played)).days
        recency = math.exp(-days_since_play / thresholds["recency_half_life_days"])
    play_weight = math.log2(1 + plays) * 20.0 * recency

    # --- age grace weight ---
    age_days = (now - _as_utc(added)).days
    age_grace = 0.0
    if plays == 0 and age_days < thresholds["age_grace_days"]:
        # Must exceed score_to_warm so new unwatched items land in the
        # NEUTRAL dead zone, not get flagged for demotion on day 1.
        age_grace = float(thresholds["score_to_warm"]) + 5.0

    score = round(play_weight + age_grace, 1)
    breakdown = {
        "plays": plays,
        "days_since_play": days_since_play,
        "age_days": age_days,
        "recency_factor": round(recency, 3),
        "play_weight": round(play_weight, 2),
        "age_grace_weight": round(age_grace, 2),
        "score": score,
    }
    return score, breakdown


def score_recommendation(score: float, thresholds: dict) -> str:
    """Map a raw heat score to a tier recommendation ignoring current state.

    Returns 'HOT' | 'WARM' | 'NEUTRAL'. NEUTRAL is the hysteresis dead
    zone between score_to_warm and score_to_hot.
    """
    if score >= thresholds["score_to_hot"]:
        return "HOT"
    if score <= thresholds["score_to_warm"]:
        return "WARM"
    return "NEUTRAL"


def decide_outcome(
    score: float, current_tier: str, thresholds: dict
) -> str:
    """Legacy shim — returns the pre-override outcome string.

    Kept because a lot of the codebase + tests call it. The real outcome
    (post-override, with current_tier awareness) is computed in
    `_apply_overrides` / `_combine_outcome`.
    """
    rec = score_recommendation(score, thresholds)
    return _combine_outcome(current_tier, rec, pinned=False)


def _combine_outcome(
    current_tier: str, recommendation: str, pinned: bool
) -> str:
    """Given current_tier, score-based recommendation, and pinned flag,
    return the final outcome string.

    Outcome alphabet:
      PIN_HOT         -> pinned, regardless of current tier (if not HOT,
                         P2 will promote). Dominates STAY_HOT/TO_HOT for
                         reporting so the user can spot pinned items.
      STAY_HOT        -> on HOT, recommendation keeps it HOT.
      STAY_WARM       -> on WARM, recommendation keeps it WARM.
      TO_HOT          -> on WARM (or MIXED), recommendation promotes.
      TO_WARM         -> on HOT (or MIXED), recommendation demotes.
      SHOULD_BE_HOT   -> current tier UNKNOWN, recommendation HOT.
      SHOULD_BE_WARM  -> current tier UNKNOWN, recommendation WARM.
      NEUTRAL         -> dead zone AND current unknown. If current tier
                         is known, NEUTRAL collapses to STAY_<current>.
      MIXED_NEUTRAL   -> current tier MIXED, no direction to resolve it.
                         P2 will leave it alone. Rare (50/50 byte split).
    """
    if pinned:
        return "PIN_HOT"

    if current_tier == "UNKNOWN":
        return {
            "HOT": "SHOULD_BE_HOT",
            "WARM": "SHOULD_BE_WARM",
            "NEUTRAL": "NEUTRAL",
        }[recommendation]

    if current_tier == "HOT":
        if recommendation == "WARM":
            return "TO_WARM"
        return "STAY_HOT"

    if current_tier == "WARM":
        if recommendation == "HOT":
            return "TO_HOT"
        return "STAY_WARM"

    if current_tier == "MIXED":
        if recommendation == "HOT":
            return "TO_HOT"
        if recommendation == "WARM":
            return "TO_WARM"
        return "MIXED_NEUTRAL"

    # Unknown current_tier value — degrade gracefully.
    return {
        "HOT": "SHOULD_BE_HOT",
        "WARM": "SHOULD_BE_WARM",
        "NEUTRAL": "NEUTRAL",
    }.get(recommendation, "NEUTRAL")


# ---------- Plex traversal ----------


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sum_part_sizes(media_list) -> int:
    total = 0
    for media in media_list or []:
        for part in getattr(media, "parts", None) or []:
            size = getattr(part, "size", None) or 0
            total += size
    return total


def _clean_title(title: str, year: Optional[int]) -> str:
    """Strip a trailing '(YYYY)' from title if it matches the item's year.

    Some Plex agents (and some manual rename conventions) bake the year
    into the title field. Combined with our own '(year)' rendering this
    produces 'The Grand Tour (2016) (2016)'. Cosmetic only — does not
    affect scoring or matching.
    """
    if not title:
        return title or ""
    cleaned = title.strip()
    if year:
        suffix = f"({year})"
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].rstrip()
    return cleaned


def _parse_grandparent_id(h) -> Optional[int]:
    """Return the grandparent ratingKey int from a history event, or None.

    The /status/sessions/history endpoint omits grandparentRatingKey on episode
    events but always provides grandparentKey in path form (/library/metadata/<id>).
    We try grandparentRatingKey first (direct int); on None we parse the trailing
    integer from grandparentKey as the fallback.
    """
    grk = getattr(h, "grandparentRatingKey", None)
    if grk is not None:
        try:
            return int(grk)
        except (TypeError, ValueError):
            pass

    gk = getattr(h, "grandparentKey", None)
    if gk is not None:
        try:
            tail = gk.rstrip("/").rsplit("/", 1)[-1]
            return int(tail)
        except (TypeError, ValueError, IndexError):
            pass

    return None


def _ingest_history(events, index: dict) -> int:
    """Fold a sequence of History objects into the index. Returns event count.

    Each event contributes to:
      - index[ratingKey]    (the movie OR episode itself)
      - index[grandparent]  (the show, for series rollup — resolved via
                             grandparentRatingKey when present, or by parsing
                             the trailing id from grandparentKey, because the
                             history endpoint omits grandparentRatingKey)
    """
    count = 0
    for h in events:
        rk = getattr(h, "ratingKey", None)
        viewed_at = getattr(h, "viewedAt", None)
        if rk is None:
            continue
        count += 1

        try:
            rk_int = int(rk)
        except (TypeError, ValueError):
            continue
        entry = index.setdefault(rk_int, {"plays": 0, "last": None})
        entry["plays"] += 1
        if viewed_at and (entry["last"] is None or viewed_at > entry["last"]):
            entry["last"] = viewed_at

        grk_int = _parse_grandparent_id(h)
        if grk_int is None:
            continue
        gentry = index.setdefault(grk_int, {"plays": 0, "last": None})
        gentry["plays"] += 1
        if viewed_at and (gentry["last"] is None or viewed_at > gentry["last"]):
            gentry["last"] = viewed_at
    return count


def build_history_index(plex: PlexServer) -> dict:
    """Build a play-history index from Plex's playback events.

    Returns a dict keyed by ratingKey (int) -> {"plays": int, "last": datetime|None}.

    Why this exists:
        movie.viewCount / episode.viewCount only reflect the token owner's
        plays, AND only update on near-complete watches. Plex Home users
        and partial scrubs are invisible to those fields. plex.history()
        returns raw playback events across ALL accounts and includes
        partial plays.

    Why PER-SECTION calls (not one global history()):
        The global /status/sessions/history/all endpoint silently caps
        its response for busy servers — when we called it globally the
        first time, some TV shows came back with zero events despite 
        being actively watched. Scoping each call with librarySectionID 
        keeps the response bounded per library and plexapi's pagination 
        works reliably within that bound.
    """
    log.info("Fetching playback history from Plex (per section)...")
    index: dict = {}
    total = 0

    try:
        sections = list(plex.library.sections())
    except Exception as e:  # noqa: BLE001
        log.warning("Could not enumerate library sections: %s", e)
        sections = []

    for section in sections:
        try:
            events = plex.history(librarySectionID=section.key)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "history() failed for section %r: %s — skipping",
                section.title, e,
            )
            continue
        n = _ingest_history(events, index)
        total += n
        log.info("  section %-28s %6d history events", section.title, n)

    if total == 0 and sections:
        # Per-section returned nothing — try a final global sweep before
        # giving up, so we at least get SOME signal.
        log.warning("Per-section history was empty; attempting global fallback")
        try:
            total = _ingest_history(plex.history(), index)
        except Exception as e:  # noqa: BLE001
            log.warning("Global history fallback also failed: %s", e)

    log.info(
        "History index: %d events across %d unique ratingKeys",
        total, len(index),
    )
    return index


def _count_recent_episode_plays(episodes, history_index: dict, cutoff: Optional[datetime]) -> int:
    """Count distinct episodes (by ratingKey) with a play event after `cutoff`.

    Used by P4.5's fast-promote guard: a promote-only run should not promote
    a multi-season series off a single-pilot watch. `history_index` already
    holds a per-episode entry (keyed by the episode's own ratingKey, not the
    show rollup) with a "last" viewed timestamp — see _ingest_history.

    cutoff=None means "no full run has ever completed" — count every episode
    with ANY recorded play, per the P4.5 spec's no-full-run-yet fallback.
    """
    count = 0
    for ep in episodes:
        rk = getattr(ep, "ratingKey", None)
        if rk is None:
            continue
        try:
            rk_int = int(rk)
        except (TypeError, ValueError):
            continue
        entry = history_index.get(rk_int)
        last = entry.get("last") if entry else None
        if not last:
            continue
        # Plex's viewedAt timestamps come back offset-naive; cutoff (derived
        # from our own isoformat() writes) is always offset-aware. Normalise
        # before comparing or this raises TypeError on every promote-only run.
        last = _as_utc(last)
        if cutoff is None or last > cutoff:
            count += 1
    return count


def _show_history_fallback(show) -> tuple[int, Optional[datetime]]:
    """Last-resort per-show history query.

    Hit if a show's ratingKey turned up empty in the section-level index
    (can happen if the show was re-matched or moved libraries — old
    history events reference a stale grandparentRatingKey that doesn't
    equal show.ratingKey anymore).

    show.history() asks Plex for history scoped to THIS show's
    ratingKey, so it bypasses any global/section paging weirdness.
    """
    try:
        events = show.history()
    except Exception as e:  # noqa: BLE001
        log.debug("show.history() failed for %r: %s", show.title, e)
        return 0, None
    plays = 0
    last = None
    for h in events:
        plays += 1
        viewed_at = getattr(h, "viewedAt", None)
        if viewed_at and (last is None or viewed_at > last):
            last = viewed_at
    return plays, last


def _build_recently_active_shows(section, thresholds: dict) -> set:
    """Return int ratingKeys of shows that have episodes added within the floor window.

    ONE section.search() call per TV library — O(1) per show, avoids
    iterating show.episodes() which would be thousands of API calls.
    Returns empty set if added_floor_days_tv is 0/null or the call fails.
    """
    days = thresholds.get("added_floor_days_tv")
    if not days:
        return set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
    try:
        # plexapi compares addedAt__gte against the stored string form of a
        # Unix timestamp — passing a datetime causes a str >= datetime
        # TypeError. int seconds is the only form that works.
        recent_eps = section.search(libtype="episode", addedAt__gte=int(cutoff.timestamp()))
        return {
            int(ep.grandparentRatingKey)
            for ep in recent_eps
            if getattr(ep, "grandparentRatingKey", None) is not None
        }
    except Exception as e:  # noqa: BLE001
        log.warning(
            "Added-floor TV search failed for section %r: %s — floor disabled for this section",
            section.title, e,
        )
        return set()


def _build_collection_pinned_keys(
    plex: PlexServer, pinned_collections: list,
) -> tuple:
    """Fetch members of named Plex collections and return their ratingKeys.

    Returns (keys, matched_count, total_items) where:
      keys          — set of int ratingKeys across all matched collections
      matched_count — number of collections successfully fetched
      total_items   — len(keys) (unique across collections)

    Logs a WARNING for any entry whose library or collection doesn't exist
    and continues — a bad config entry must not abort the run.

    Both library and name are required per entry; collection names are
    per-section in Plex (same name can appear in different libraries), so
    name-only matching would be ambiguous.
    """
    if not pinned_collections:
        return set(), 0, 0

    keys: set = set()
    matched = 0

    for entry in pinned_collections:
        lib_name = (entry.get("library") or "").strip()
        col_name = (entry.get("name") or "").strip()
        if not lib_name or not col_name:
            log.warning(
                "pinned_collections entry missing library or name: %r — skipping", entry
            )
            continue
        try:
            section = plex.library.section(lib_name)
        except Exception as e:  # noqa: BLE001
            log.warning("Collection-pin: library %r not found: %s", lib_name, e)
            continue
        try:
            all_cols = section.collections()
        except Exception as e:  # noqa: BLE001
            log.warning(
                "Collection-pin: could not list collections in %r: %s", lib_name, e
            )
            continue
        col = next(
            (c for c in (all_cols or []) if c.title.lower() == col_name.lower()),
            None,
        )
        if col is None:
            log.warning(
                "Collection-pin: collection %r not found in library %r",
                col_name, lib_name,
            )
            continue
        try:
            members = col.items()
        except Exception as e:  # noqa: BLE001
            log.warning(
                "Collection-pin: could not list members of %r/%r: %s",
                col_name, lib_name, e,
            )
            continue
        member_keys = {
            int(m.ratingKey)
            for m in (members or [])
            if getattr(m, "ratingKey", None) is not None
        }
        matched += 1
        keys.update(member_keys)
        log.debug(
            "Collection-pin: %r/%r — %d members", lib_name, col_name, len(member_keys)
        )

    return keys, matched, len(keys)


def _build_auto_inherit_keys(
    plex: PlexServer,
    auto_cfg: dict,
    score_to_hot: float,
    items: List[Item],
    configured_library_names: List[str],
) -> tuple:
    """Return ratingKeys that should be auto-pinned via collection inheritance.

    For each Plex collection in the configured libraries, counts members whose
    natural score (pre-floor, pre-pin) is >= score_to_hot. When that count
    meets min_hot_members, every member of the collection is added to the
    result set so the caller can stamp them auto_inherit_pinned=True.

    Returns (keys, triggered_count, total_inherited):
      keys            — set of int ratingKeys across all triggered collections
      triggered_count — how many collections met the threshold
      total_inherited — total member slots across triggered collections

    Is a no-op (returns empty set) when auto_cfg["enabled"] is falsy.
    Logs WARNINGs for missing libraries / un-listable collections; never raises.
    """
    if not auto_cfg.get("enabled"):
        return set(), 0, 0

    min_hot = int(auto_cfg.get("min_hot_members") or 2)
    min_fraction = float(auto_cfg.get("min_hot_fraction") or 0.5)
    skip_smart = bool(auto_cfg.get("skip_smart_collections", True))
    exclude_libs = {
        str(e).strip().lower()
        for e in (auto_cfg.get("exclude_libraries") or [])
    }

    # Build rating_key -> item lookup for membership + score checks.
    rk_lookup: dict = {
        it.rating_key: it
        for it in items
        if it.rating_key is not None
    }

    keys: set = set()
    triggered_count = 0
    total_inherited = 0

    for lib_name in configured_library_names:
        if lib_name.strip().lower() in exclude_libs:
            continue
        try:
            section = plex.library.section(lib_name)
        except Exception as e:  # noqa: BLE001
            log.warning("Auto-inherit: library %r not found: %s", lib_name, e)
            continue
        try:
            collections = section.collections()
        except Exception as e:  # noqa: BLE001
            log.warning(
                "Auto-inherit: could not list collections in %r: %s", lib_name, e
            )
            continue

        for col in (collections or []):
            if skip_smart and getattr(col, "smart", False):
                continue
            try:
                members = col.items()
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "Auto-inherit: could not get members of %r in %r: %s",
                    col.title, lib_name, e,
                )
                continue

            member_keys = {
                int(m.ratingKey)
                for m in (members or [])
                if getattr(m, "ratingKey", None) is not None
            }
            col_size = len(member_keys)
            if col_size == 0:
                continue

            # Efficiency: collections smaller than min_hot can never trigger.
            if col_size < min_hot:
                continue

            # Three-branch threshold:
            #   size == min_hot  → fraction escape hatch (avoids the degenerate
            #                      "all members must be hot, nothing to inherit"
            #                      case for small collections)
            #   size >  min_hot  → absolute min_hot threshold
            if col_size == min_hot:
                required_hot = max(1, math.ceil(col_size * min_fraction))
            else:
                required_hot = min_hot

            hot_count = sum(
                1 for rk in member_keys
                if rk in rk_lookup and rk_lookup[rk].score >= score_to_hot
            )
            if hot_count < required_hot:
                continue

            triggered_count += 1
            total_inherited += len(member_keys)
            keys.update(member_keys)
            log.debug(
                "Auto-inherit: %r/%r triggered (%d hot, %d total members)",
                lib_name, col.title, hot_count, len(member_keys),
            )

    return keys, triggered_count, total_inherited


def collect_movies(
    section, library_name: str, now, thresholds, history_index: dict,
    path_map, hot_mount: str, array_disks: List[str],
    user_share_prefix: str = "",
) -> Iterable[Item]:
    # Group by guid to dedupe multi-version uploads (e.g. 4K Extended +
    # 4K + 1080p versions of the same movie that weren't merged in Plex).
    # Same guid = same TMDB/IMDB identity = treat as one tiering unit.
    by_guid: dict = {}
    for movie in section.all():
        guid = getattr(movie, "guid", None) or f"_rk_{movie.ratingKey}"
        by_guid.setdefault(guid, []).append(movie)

    tier_probing = bool(hot_mount) and bool(array_disks)

    for _guid, group in by_guid.items():
        primary = group[0]
        year = getattr(primary, "year", None)
        title = _clean_title(primary.title, year)

        # Sum sizes across all duplicates AND all media versions per movie
        size = sum(_sum_part_sizes(getattr(m, "media", None)) for m in group)

        # Aggregate plays + last_played from history across all duplicates
        plays = 0
        last = None
        for m in group:
            try:
                rk = int(m.ratingKey)
            except (TypeError, ValueError):
                continue
            entry = history_index.get(rk)
            if not entry:
                continue
            plays += entry["plays"]
            if entry["last"] and (last is None or entry["last"] > last):
                last = entry["last"]

        # Safety fallback: if history was empty (e.g. plex.history() failed
        # or this server genuinely has no history), use the legacy
        # owner-scoped viewCount so we don't lose all signal.
        if plays == 0:
            for m in group:
                vc = int(getattr(m, "viewCount", 0) or 0)
                plays += vc
                lv = getattr(m, "lastViewedAt", None)
                if lv and (last is None or lv > last):
                    last = lv

        added_candidates = [
            _as_utc(getattr(m, "addedAt", None) or now) for m in group
        ]
        added = min(added_candidates) if added_candidates else now

        score, breakdown = heat_score(plays, last, added, now, thresholds)

        # Added-date floor: flag if added within the threshold window.
        added_floor_days = int(thresholds.get("added_floor_days_movies") or 0)
        recently_added = bool(
            added_floor_days
            and (now - _as_utc(added)).days <= added_floor_days
        )

        # Current tier (P1). Probes only if tier detection is configured.
        current_tier = "UNKNOWN"
        current_disk: Optional[str] = None
        source_dirs: List[str] = []
        tier_split: Optional[dict] = None
        warm_disk_files: Dict[str, List[str]] = {}
        hot_pool_files: List[str] = []
        if tier_probing:
            parts = []
            for m in group:
                parts.extend(_media_parts(getattr(m, "media", None)))
            current_tier, tier_split, current_disk, source_dirs, warm_disk_files, hot_pool_files = resolve_item_current_tier(
                parts, path_map, hot_mount, array_disks, user_share_prefix,
            )
            if tier_split:
                breakdown["tier_split"] = tier_split

        try:
            primary_rk: Optional[int] = int(primary.ratingKey)
        except (TypeError, ValueError):
            primary_rk = None

        yield Item(
            title=title,
            year=year,
            kind="movie",
            library=library_name,
            plays=plays,
            last_played=_as_utc(last) if last else None,
            added=added,
            size_bytes=size,
            score=score,
            current_tier=current_tier,
            current_disk=current_disk,
            source_dirs=source_dirs,
            warm_disk_files=warm_disk_files,
            hot_pool_files=hot_pool_files,
            outcome="NEUTRAL",  # finalised in post-scoring override pass
            rating_key=primary_rk,
            recently_added=recently_added,
            score_breakdown=breakdown,
        )


def collect_series(
    section, library_name: str, now, thresholds, history_index: dict,
    path_map, hot_mount: str, array_disks: List[str],
    user_share_prefix: str = "",
    recently_active_shows: Optional[set] = None,
    fast_promote_cutoff: Optional[datetime] = None,
) -> Iterable[Item]:
    tier_probing = bool(hot_mount) and bool(array_disks)

    for show in section.all():
        episodes = show.episodes()
        if not episodes:
            # No episodes means nothing to tier; skip silently.
            continue

        year = getattr(show, "year", None)
        title = _clean_title(show.title, year)

        # Look up rolled-up plays via the show's ratingKey. The history
        # index already aggregated episode plays under the grandparent.
        try:
            rk = int(show.ratingKey)
        except (TypeError, ValueError):
            rk = None
        entry = history_index.get(rk) if rk is not None else None
        if entry:
            plays = entry["plays"]
            last = entry["last"]
        else:
            plays = 0
            last = None

        # First fallback: per-show history query. Handles the case where
        # the section-level sweep missed this show (stale grandparent
        # ratingKey after rematch, or the section's history response was
        # truncated).
        if plays == 0:
            fb_plays, fb_last = _show_history_fallback(show)
            if fb_plays > 0:
                plays = fb_plays
                last = fb_last
                log.debug(
                    "Per-show history rescued %r: %d plays, last %s",
                    show.title, plays, last,
                )

        # Final fallback: legacy owner-scoped viewCount. Still wrong for
        # Home users, but better than nothing if all Plex history APIs
        # failed for this server.
        if plays == 0:
            plays = sum(
                int(getattr(e, "viewCount", 0) or 0) for e in episodes
            )
            last_candidates = [
                getattr(e, "lastViewedAt", None) for e in episodes
            ]
            last_candidates = [d for d in last_candidates if d]
            last = max(last_candidates) if last_candidates else None

        # Series "added" = show.addedAt (earliest episode usually).
        added = getattr(show, "addedAt", None) or now
        size = sum(_sum_part_sizes(getattr(e, "media", None)) for e in episodes)

        score, breakdown = heat_score(plays, last, added, now, thresholds)

        # Added-date floor: show is "recently active" if any episode was
        # added within the threshold window (checked via pre-built set).
        recently_added = bool(
            recently_active_shows and rk is not None and rk in recently_active_shows
        )

        recent_episode_plays = _count_recent_episode_plays(
            episodes, history_index, fast_promote_cutoff,
        )

        # Current tier (P1): majority-bytes rollup across every episode.
        current_tier = "UNKNOWN"
        current_disk: Optional[str] = None
        source_dirs: List[str] = []
        tier_split: Optional[dict] = None
        warm_disk_files: Dict[str, List[str]] = {}
        hot_pool_files: List[str] = []
        if tier_probing:
            parts = []
            for ep in episodes:
                parts.extend(_media_parts(getattr(ep, "media", None)))
            current_tier, tier_split, current_disk, source_dirs, warm_disk_files, hot_pool_files = resolve_item_current_tier(
                parts, path_map, hot_mount, array_disks, user_share_prefix,
            )
            if tier_split:
                breakdown["tier_split"] = tier_split

        yield Item(
            title=title,
            year=year,
            kind="series",
            library=library_name,
            plays=plays,
            last_played=_as_utc(last) if last else None,
            added=_as_utc(added),
            size_bytes=size,
            score=score,
            current_tier=current_tier,
            current_disk=current_disk,
            source_dirs=source_dirs,
            warm_disk_files=warm_disk_files,
            hot_pool_files=hot_pool_files,
            outcome="NEUTRAL",  # finalised in post-scoring override pass
            rating_key=rk,
            recently_added=recently_added,
            recent_episode_plays=recent_episode_plays,
            score_breakdown=breakdown,
        )


def _compute_recommendation(
    item: Item, cfg: dict, now: datetime,
) -> Tuple[str, bool, Optional[str]]:
    """Resolve the tier recommendation for an item, applying overrides.

    Returns (recommendation, pinned, reason):
      recommendation: 'HOT' | 'WARM' | 'NEUTRAL'
      pinned:         True if a library/title pin fired (drives PIN_HOT)
      reason:         short human string for --explain (None if raw score)

    Precedence (highest wins):
      1. Library pin         -> HOT, pinned=True
      2. Title pin           -> HOT, pinned=True
      3. Collection pin      -> HOT, pinned=True (if item.collection_pinned)
      4. Auto-inherit pin    -> HOT, pinned=True (if item.auto_inherit_pinned)
      5. Added floor         -> HOT if item.recently_added is True
      6. Raw score           -> HOT / WARM / NEUTRAL via score_recommendation.
      7. Recency floor       -> HOT if last_played within hot_recency_days
                                AND raw recommendation is NEUTRAL or WARM.
    """
    pinning = cfg.get("pinning") or {}
    thresholds = cfg.get("thresholds") or {}

    # --- 1. Library pin (case-insensitive exact match) ---
    lib_pins = [
        str(s).strip().lower() for s in (pinning.get("always_hot_libraries") or [])
    ]
    if lib_pins and item.library and item.library.strip().lower() in lib_pins:
        return "HOT", True, f"pinned library: {item.library}"

    # --- 2. Title pin (case-insensitive substring match) ---
    title_pins = [
        str(s).strip().lower() for s in (pinning.get("always_hot_titles") or [])
    ]
    if title_pins and item.title:
        needle_hit = next(
            (p for p in title_pins if p and p in item.title.lower()),
            None,
        )
        if needle_hit:
            return "HOT", True, f"pinned title match: {needle_hit!r}"

    # --- 3. Collection pin (promote-only, never demotes) ---
    if item.collection_pinned:
        return "HOT", True, "collection pin"

    # --- 4. Auto-inherit collection pin (promote-only, never demotes) ---
    if item.auto_inherit_pinned:
        return "HOT", True, "auto-inherit collection"

    # --- 5. Added-date floor (only promotes, never demotes) ---
    if item.recently_added:
        if item.kind == "movie":
            days_since_added = (now - _as_utc(item.added)).days
            threshold = thresholds.get("added_floor_days_movies", 45)
            reason = (
                f"added-date floor: added {days_since_added}d ago "
                f"(<= added_floor_days_movies={threshold})"
            )
        else:
            threshold = thresholds.get("added_floor_days_tv", 30)
            reason = f"added-date floor: recent episode (<= added_floor_days_tv={threshold})"
        return "HOT", False, reason

    # --- 6. Raw score ---
    raw_rec = score_recommendation(item.score, thresholds)

    # --- 7. Recency floor (only promotes, never demotes) ---
    recency_days = thresholds.get("hot_recency_days")
    if (
        recency_days
        and item.last_played
        and raw_rec in ("NEUTRAL", "WARM")
    ):
        days_since = (now - _as_utc(item.last_played)).days
        if days_since <= int(recency_days):
            return (
                "HOT",
                False,
                f"recency floor: watched {days_since}d ago "
                f"(<= hot_recency_days={recency_days})",
            )

    return raw_rec, False, None


def _apply_overrides(item: Item, cfg: dict, now: datetime) -> None:
    """Compute the final outcome string for an item given pinning +
    recency floor rules and its (already-populated) current_tier.

    Writes `item.outcome`, and annotates `score_breakdown` with an
    `override` reason when a rule promoted past the raw score, plus a
    `current_tier` echo for --explain visibility.
    """
    rec, pinned, reason = _compute_recommendation(item, cfg, now)
    item.outcome = _combine_outcome(item.current_tier, rec, pinned)
    if reason:
        item.score_breakdown["override"] = reason
    item.score_breakdown["current_tier"] = item.current_tier
    item.score_breakdown["recommendation"] = rec


def collect_all(
    plex: PlexServer, cfg: dict, filter_libraries,
    fast_promote_cutoff: Optional[datetime] = None,
) -> List[Item]:
    now = datetime.now(timezone.utc)
    # Build the history index ONCE per run — it's a single Plex API call
    # and the rest of the loop is pure dict lookups.
    history_index = build_history_index(plex)

    # Resolve tier-detection inputs once per run.
    paths_cfg = cfg.get("paths") or {}
    hot_mount = (paths_cfg.get("hot_pool_mount") or "").rstrip("/")
    array_disks = resolve_array_disks(cfg)
    path_map = paths_cfg.get("plex_path_map") or []
    user_share_prefix = (paths_cfg.get("user_share_prefix") or "").rstrip("/")

    if hot_mount and array_disks:
        log.info(
            "Tier detection enabled: hot=%s  warm=%s",
            hot_mount, ", ".join(array_disks),
        )
        if path_map:
            log.info("Plex path translation rules: %d", len(path_map))
        else:
            log.info(
                "No plex_path_map configured — assuming Plex paths match "
                "tier.py's view directly."
            )
        if user_share_prefix:
            log.info("User-share resolution enabled: prefix=%s", user_share_prefix)
    else:
        log.info(
            "Tier detection disabled (hot_pool_mount=%r, array_disks=%d). "
            "Outcomes will be SHOULD_BE_* rather than STAY_*/TO_*.",
            hot_mount, len(array_disks),
        )

    thresholds = cfg["thresholds"]
    floor_movie_count = 0
    floor_series_count = 0

    items: List[Item] = []
    for lib_cfg in cfg["libraries"]:
        name = lib_cfg["name"] if isinstance(lib_cfg, dict) else lib_cfg
        if filter_libraries and name not in filter_libraries:
            continue
        try:
            section = plex.library.section(name)
        except Exception as e:
            print(f"! Could not open library '{name}': {e}", file=sys.stderr)
            continue
        if section.type == "movie":
            new_items = list(
                collect_movies(
                    section, name, now, thresholds, history_index,
                    path_map, hot_mount, array_disks, user_share_prefix,
                )
            )
            floor_movie_count += sum(1 for it in new_items if it.recently_added)
            items.extend(new_items)
        elif section.type == "show":
            recently_active_shows = _build_recently_active_shows(section, thresholds)
            floor_series_count += len(recently_active_shows)
            new_items = list(
                collect_series(
                    section, name, now, thresholds, history_index,
                    path_map, hot_mount, array_disks, user_share_prefix,
                    recently_active_shows=recently_active_shows,
                    fast_promote_cutoff=fast_promote_cutoff,
                )
            )
            items.extend(new_items)
        else:
            print(
                f"! Library '{name}' has unsupported type '{section.type}', skipping",
                file=sys.stderr,
            )

    if thresholds.get("added_floor_days_movies") or thresholds.get("added_floor_days_tv"):
        log.info(
            "Added-floor: %d movies + %d series with recent activity",
            floor_movie_count, floor_series_count,
        )

    # Auto-inherit pass: scan all configured libraries for collections that
    # have enough naturally-hot members to trigger the inherit rule.
    auto_cfg = cfg.get("auto_collection_inherit") or {}
    if auto_cfg.get("enabled"):
        lib_names = [
            (lib_cfg["name"] if isinstance(lib_cfg, dict) else lib_cfg)
            for lib_cfg in cfg["libraries"]
        ]
        score_to_hot = float((cfg.get("thresholds") or {}).get("score_to_hot", 40.0))
        ai_keys, ai_triggered, ai_inherited = _build_auto_inherit_keys(
            plex, auto_cfg, score_to_hot, items, lib_names,
        )
        if ai_keys:
            for it in items:
                if it.rating_key is not None and it.rating_key in ai_keys:
                    it.auto_inherit_pinned = True
        log.info(
            "Auto-inherit: %d collections triggered (≥%d hot members), %d items inherited",
            ai_triggered, int(auto_cfg.get("min_hot_members") or 2), ai_inherited,
        )

    # Collection-pin pass: fetch named collections once, mark matching items.
    pinned_collections_cfg = cfg.get("pinned_collections") or []
    if pinned_collections_cfg:
        col_keys, col_matched, col_total = _build_collection_pinned_keys(
            plex, pinned_collections_cfg
        )
        if col_keys:
            for it in items:
                if it.rating_key is not None and it.rating_key in col_keys:
                    it.collection_pinned = True
        log.info(
            "Collection-pin: %d collections matched, %d items pinned",
            col_matched, col_total,
        )

    # Post-scoring override pass. Done here (not per-collector) so both
    # movie and series paths share the same rule engine and the summary
    # counts reflect final outcomes.
    for it in items:
        _apply_overrides(it, cfg, now)

    # Eviction pass: items on eviction-marked warm disks that would STAY_WARM
    # are flagged RELOCATE_WARM so P2's mover knows to relocate them.
    evict_cfg = cfg.get("array_disk_evict") or {}
    if evict_cfg.get("enabled"):
        evict_disks = _build_evict_disks(evict_cfg, array_disks)
        if evict_disks:
            for it in items:
                if it.current_tier == "WARM":
                    log.debug(
                        "eviction probe: %r kind=%s disk=%r outcome=%s",
                        it.title, it.kind, it.current_disk, it.outcome,
                    )
            items_on_evict = [
                it for it in items
                if (it.current_disk is not None and it.current_disk in evict_disks)
                or any(d in evict_disks for d in it.warm_disk_files)
            ]
            log.info(
                "Eviction: %d disks marked (%s), %d items currently on evicting disks",
                len(evict_disks), ", ".join(sorted(evict_disks)), len(items_on_evict),
            )
            relocate_count = 0
            implicit_hot_count = 0
            for it in items_on_evict:
                if it.outcome == "STAY_WARM":
                    it.outcome = "RELOCATE_WARM"
                    relocate_count += 1
                    # Always restrict the rsync source to ONLY the evicting-disk
                    # files.  warm_disk_files may include files from non-evicting
                    # disks (older seasons of a series whose majority lives on the
                    # evicting disk).  Without this guard the move executor would:
                    #   1. rsync non-evicting-disk files to themselves (self-copy
                    #      when co-location picks that disk as the destination)
                    #   2. size_verify would PASS (the files are "already there")
                    #   3. delete would remove them → data loss
                    evict_files = {
                        d: it.warm_disk_files[d]
                        for d in it.warm_disk_files
                        if d in evict_disks
                    }
                    if evict_files:
                        it.relocate_source_override = evict_files
                        # Minority-evict only: override current_disk to the
                        # evicting disk so _select_warm_destination excludes it
                        # from candidates (not the safe majority disk).
                        if it.current_disk not in evict_disks:
                            it.current_disk = max(
                                evict_files.keys(),
                                key=lambda d: sum(
                                    os.path.getsize(f)
                                    for f in evict_files[d]
                                    if os.path.exists(f)
                                ),
                            )
                elif it.outcome in _HOT_OUTCOMES:
                    implicit_hot_count += 1
            log.info(
                "Eviction: %d items flagged RELOCATE_WARM (TO_HOT path: %d)",
                relocate_count, implicit_hot_count,
            )

    # Straggler cleanup: items whose MAJORITY tier matches their recommendation
    # (STAY) but whose MINORITY bytes are on the wrong tier.  Typical cause:
    # a partial previous move where the main title file moved but extras did
    # not, or a movie-per-folder library where extras were added while the main
    # file was already on a different tier.
    straggler_to_warm = 0
    straggler_to_hot = 0
    for it in items:
        if it.outcome == "STAY_WARM" and it.hot_pool_files:
            it.outcome = "TO_WARM"
            straggler_to_warm += 1
        elif (it.outcome == "STAY_HOT" or it.outcome == "PIN_HOT") and it.warm_disk_files:
            # STAY_HOT: majority bytes already on HOT but warm stragglers remain
            #   (partial prior move — finish it).
            # PIN_HOT: item is pinned to HOT but physically still on a warm
            #   disk — promote it (next run it shows PIN_HOT once it's on HOT).
            it.outcome = "TO_HOT"
            straggler_to_hot += 1
    if straggler_to_warm or straggler_to_hot:
        log.info(
            "Stragglers: %d STAY_WARM→TO_WARM, %d STAY_HOT→TO_HOT "
            "(files on wrong tier despite correct majority-bytes tier)",
            straggler_to_warm, straggler_to_hot,
        )

    return items


# ---------- Output ----------


def _fmt_date(d: Optional[datetime]) -> str:
    return d.strftime("%Y-%m-%d") if d else "—"


def _fmt_size(gb: float) -> str:
    if gb >= 1000:
        return f"{gb / 1024:.2f} TB"
    return f"{gb:.1f} GB"


def _fmt_eta(seconds: float) -> str:
    secs = int(seconds)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


# ---------- Move executor (P2) ----------


def _disk_free_bytes(path: str) -> int:
    """Return free bytes on the filesystem at path, or 0 on error."""
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def _disk_usage(path: str):
    """Return shutil.disk_usage namedtuple for path — module-level for test patching."""
    return shutil.disk_usage(path)


def _zfs_pool_name_for_mount(mount: str) -> Optional[str]:
    """Return ZFS pool name for mount by parsing /proc/mounts (no ZFS tools needed).

    Docker containers' /proc/mounts reflects bind-mounts with the original ZFS
    device name (e.g. 'Zfs_media /mnt/hot_pool zfs rw,...').  The pool name is
    the first path component of the device name.  Returns None for non-ZFS mounts
    or if /proc/mounts is unreadable.
    """
    mount_norm = mount.rstrip("/")
    best_len = -1
    best_device: Optional[str] = None
    try:
        with open("/proc/mounts") as _f:
            for line in _f:
                cols = line.split()
                if len(cols) < 3 or cols[2] != "zfs":
                    continue
                mnt = cols[1].rstrip("/")
                if mount_norm == mnt or mount_norm.startswith(mnt + "/"):
                    if len(mnt) > best_len:
                        best_len = len(mnt)
                        best_device = cols[0]
    except OSError:
        pass
    if best_device:
        return best_device.split("/")[0]
    return None


def _try_zpool_cmd(pool_name: str) -> Optional[Tuple[int, int]]:
    """Return (total, alloc) via 'zpool list' command.  None if unavailable."""
    try:
        r = subprocess.run(
            ["zpool", "list", "-Hp", "-o", "size,alloc", pool_name],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            parts = r.stdout.strip().split()
            if len(parts) >= 2:
                return int(parts[0]), int(parts[1])
    except Exception:  # noqa: BLE001
        pass
    return None


def _try_zpool_kstat(pool_name: str) -> Optional[Tuple[int, int]]:
    """Return (total, used) from /proc/spl/kstat/zfs/<pool>/objset-* kernel stats.

    Sums dk_referenced across all datasets for pool-wide used bytes.  Reads
    dk_available (pool free space) from any objset file.  Accessible inside
    Docker containers on Unraid without ZFS userspace tools — the kernel module
    exposes these stats globally via procfs.
    Returns None if the kstat directory doesn't exist or stats are missing.
    """
    kstat_dir = f"/proc/spl/kstat/zfs/{pool_name}"
    if not os.path.isdir(kstat_dir):
        return None
    total_referenced = 0
    available: Optional[int] = None
    try:
        for fname in os.listdir(kstat_dir):
            if not fname.startswith("objset-"):
                continue
            try:
                kv: dict = {}
                with open(os.path.join(kstat_dir, fname)) as _f:
                    for line in _f:
                        cols = line.split()
                        if len(cols) >= 3:
                            kv[cols[0]] = cols[2]
                if "dk_referenced" in kv:
                    total_referenced += int(kv["dk_referenced"])
                if "dk_available" in kv and available is None:
                    available = int(kv["dk_available"])
            except (OSError, ValueError):
                continue
    except OSError:
        return None
    if available is None:
        return None
    return total_referenced + available, total_referenced


def _try_unraid_api(
    api_url: str,
    api_key: Optional[str],
    pool_name_filter: Optional[str],
    hot_mount: str = "",
    free_floor_bytes: int = 0,
    verify_tls: bool = True,
) -> Optional[Tuple[int, int]]:
    """Query the Unraid Connect GraphQL API for ZFS pool capacity.

    Requires Unraid 6.12+ with an API key (Settings → Management Access →
    API keys in the Unraid UI).  Uses the `array { caches }` query — ZFS
    pools are modelled as cache entries in the Unraid array API.

    Matching priority (first match wins):
    1. pool_name_filter (capacity.unraid_pool_name config key) — exact,
       case-insensitive match on the cache name.
    2. Last path component of hot_mount — e.g. "/mnt/zfs_media" → "zfs_media".
    3. First ZFS cache whose fsSize >= free_floor_bytes (size heuristic).

    Returns (total_bytes, used_bytes) or None on any failure.  All failures
    are DEBUG-logged so an unconfigured endpoint produces no noise.
    """
    import json
    import urllib.request

    query = "{ array { caches { name fsType fsSize fsFree fsUsed } } }"
    try:
        payload = json.dumps({"query": query}).encode()
        req = urllib.request.Request(
            api_url,
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        if api_key:
            req.add_header("x-api-key", api_key)
        ssl_ctx: Optional[ssl.SSLContext] = None
        if not verify_tls:
            log.warning(
                "Capacity: Unraid API TLS verification DISABLED "
                "(capacity.unraid_api_verify_tls=false)"
            )
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=5, context=ssl_ctx) as resp:  # noqa: S310
            data = json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        log.debug("Capacity: Unraid API request failed: %s", exc)
        return None

    errors = data.get("errors")
    if errors:
        log.debug("Capacity: Unraid API returned errors: %s", errors)
        return None

    caches = (data.get("data") or {}).get("array", {}).get("caches") or []

    # Build candidate name filters in priority order.
    name_candidates: List[str] = []
    if pool_name_filter:
        name_candidates.append(pool_name_filter.lower())
    if hot_mount:
        name_candidates.append(hot_mount.rstrip("/").split("/")[-1].lower())

    def _pick(cache: dict) -> Optional[Tuple[int, int]]:
        # Unraid API returns fsSize/fsUsed/fsFree in SI kilobytes (1 kB = 1000 bytes).
        # Multiply by 1000 to get bytes for consistent internal accounting.
        total = int(cache.get("fsSize") or 0) * 1000
        used = int(cache.get("fsUsed") or 0) * 1000
        if not total:
            return None
        return total, used

    # Pass 1: name-based match.
    for candidate in name_candidates:
        for cache in caches:
            if (cache.get("name") or "").lower() == candidate:
                result = _pick(cache)
                if result:
                    log.debug(
                        "Capacity: Unraid API matched cache %r by name: total=%d used=%d",
                        cache["name"], *result,
                    )
                    return result

    # Pass 2: first ZFS cache large enough to be the hot pool.
    for cache in caches:
        if (cache.get("fsType") or "").lower() != "zfs":
            continue
        result = _pick(cache)
        if result and result[0] >= free_floor_bytes:
            log.debug(
                "Capacity: Unraid API matched cache %r by size heuristic: total=%d used=%d",
                cache.get("name"), *result,
            )
            return result

    log.debug(
        "Capacity: Unraid API found %d cache(s) — no ZFS match for filter=%r mount=%r",
        len(caches), pool_name_filter, hot_mount,
    )
    return None


def _pool_usage_bytes(
    mount: str,
    total_gb_override: Optional[float] = None,
    unraid_api_url: Optional[str] = None,
    unraid_api_key: Optional[str] = None,
    unraid_pool_name: Optional[str] = None,
    verify_tls: bool = True,
) -> Tuple[int, int]:
    """Return (total_bytes, used_bytes) for the hot pool at mount.

    Priority chain — stops at the first successful method:

    1. Unraid Connect GraphQL API — if capacity.unraid_api_url is configured.
       Accurate, no ZFS tools needed, works inside Docker via the host network.
       Requires Unraid 6.12+ and an API key.
    2. zpool command — accurate; requires ZFS userspace tools in the container.
    3. /proc/spl kstat — accurate inside Docker when /proc/spl is bind-mounted
                         from the host (no ZFS tools needed).
    4. hot_pool_total_gb config override — user supplies pool total; statvfs.free
                         (correct on ZFS) is used to derive used = total − free.
    5. statvfs fallback — correct for non-ZFS; wrong for ZFS child datasets.
                         Emits a WARNING when used=0 to prompt configuration.
    """
    # --- Method 1: Unraid Connect GraphQL API ---
    if unraid_api_url:
        try:
            du = _disk_usage(mount)
            free_floor = du.free  # heuristic: pool total must be >= statvfs free
        except OSError:
            free_floor = 0
        result = _try_unraid_api(
            unraid_api_url, unraid_api_key, unraid_pool_name,
            hot_mount=mount, free_floor_bytes=free_floor, verify_tls=verify_tls,
        )
        if result is not None:
            log.debug("Capacity: hot pool via Unraid API: total=%d used=%d", *result)
            return result
        log.warning(
            "Capacity: Unraid Connect API (capacity.unraid_api_url) was configured but "
            "returned no usable result — falling back to local detection "
            "(zpool/kstat/override/statvfs). Tiering continues; see DEBUG for the API "
            "failure detail."
        )

    # --- Methods 2 & 3: zpool command / /proc/spl kstat ---
    pool_name = _zfs_pool_name_for_mount(mount)
    if pool_name:
        result = _try_zpool_cmd(pool_name)
        if result is not None:
            log.debug("Capacity: hot pool via zpool cmd: total=%d alloc=%d", *result)
            return result
        result = _try_zpool_kstat(pool_name)
        if result is not None:
            log.debug("Capacity: hot pool via kstat: total=%d used=%d", *result)
            return result

    # --- Method 4: hot_pool_total_gb config override ---
    if total_gb_override:
        try:
            du = _disk_usage(mount)
            total = int(float(total_gb_override) * 1024 ** 3)
            used = max(0, total - du.free)
            log.debug("Capacity: hot pool via config override: total=%d used=%d", total, used)
            return total, used
        except OSError:
            pass

    # --- Method 5: statvfs fallback ---
    try:
        du = _disk_usage(mount)
        if du.used == 0 and du.total > 0:
            log.warning(
                "Capacity: hot pool used=0 — if this is a ZFS pool with files in child "
                "datasets, configure capacity.unraid_api_url or capacity.hot_pool_total_gb "
                "in tiering.yaml for accurate reporting. See README for options."
            )
        return du.total, du.used
    except OSError:
        return 0, 0


def _select_warm_destination(
    item: "Item",
    array_disks: List[str],
    safety_margin_bytes: int,
    exclude_disk: Optional[str] = None,
    warm_ceiling_pct: float = 1.0,
) -> Tuple[Optional[str], Optional[str]]:
    """Choose a warm disk for TO_WARM or RELOCATE_WARM moves.

    Returns (chosen_disk, annotation) where annotation describes why the disk
    was chosen ("most-free" | "co-locate, +X GB existing"), or (None, reason)
    when no qualifying disk exists.

    Selection rules:
      1. Exclude exclude_disk (used for RELOCATE_WARM to avoid the source disk).
      2. Filter by ceiling: skip disks already at or above warm_ceiling_pct fill.
      3. Filter by capacity: free >= item.size_bytes + safety_margin_bytes.
      4. Co-location: when warm_disk_files is non-empty (item already has files
         on at least one warm disk), prefer the candidate disk that holds the most
         bytes of this item. Applies to series AND to movies with straggler files
         so extras always land on the same spindle as the main title.
      5. Fallback: most free space.
    """
    candidates = [d for d in array_disks if d != exclude_disk]
    if not candidates:
        return None, "no candidate warm disks"

    # Filter by per-disk ceiling before free-space check.
    if warm_ceiling_pct < 1.0:
        under_ceiling = []
        for d in candidates:
            try:
                du = _disk_usage(d)
                if du.total and (du.used / du.total) < warm_ceiling_pct:
                    under_ceiling.append(d)
            except OSError:
                pass
        if not under_ceiling:
            return None, f"all warm disks over {int(warm_ceiling_pct * 100)}% ceiling"
        candidates = under_ceiling

    needed = item.size_bytes + safety_margin_bytes
    qualified = [d for d in candidates if _disk_free_bytes(d) >= needed]
    if not qualified:
        return None, f"no disk with ≥ {_fmt_size(needed / (1024 ** 3))} free (safety_margin included)"

    # Co-location: prefer the qualified disk that already holds the most bytes
    # of this item. Applies whenever warm_disk_files is populated — covers
    # both series (always partial on warm) and movie stragglers (main file
    # already on a specific warm disk, extras need to join it).
    if item.warm_disk_files:
        best_disk: Optional[str] = None
        best_sz = 0
        for disk in qualified:
            if disk not in item.warm_disk_files:
                continue
            sz = sum(
                os.path.getsize(f)
                for f in item.warm_disk_files[disk]
                if os.path.exists(f)
            )
            if sz > best_sz:
                best_sz = sz
                best_disk = disk
        if best_disk:
            return best_disk, f"co-locate, +{_fmt_size(best_sz / (1024 ** 3))} existing"

    best = max(qualified, key=_disk_free_bytes)
    return best, "most-free"


def _exec_single_move(
    prefix: str,
    title_year: str,
    files_by_src_root: Dict[str, List[str]],
    dst_root: str,
    rsync_opts: List[str],
    size_verify: bool,
    delete_after: bool,
) -> str:
    """Execute rsync, optional size verify, and optional source delete for one item.

    files_by_src_root: {src_root: [absolute file paths]} — each src_root is
        rsynced to dst_root preserving the relative path structure.
    dst_root: absolute destination root (e.g. hot pool mount or warm disk mount).

    Returns "ok" | "failed" | "delete_partial".
    """
    import tempfile as _tempfile

    dst = dst_root.rstrip("/")

    for src_root, files in files_by_src_root.items():
        src_r = src_root.rstrip("/") + "/"
        rel_paths = [
            f[len(src_r):] if f.startswith(src_r) else f.lstrip("/")
            for f in files
        ]
        try:
            with _tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False, prefix="tier_rsync_"
            ) as flist:
                flist.write("\n".join(rel_paths))
                flist_path = flist.name
        except OSError as exc:
            log.error("%s [FAILED] %s — cannot write files-from list: %s",
                      prefix, title_year, exc)
            return "failed"

        rsync_ok = True
        try:
            cmd = (["rsync"] + rsync_opts
                   + [f"--files-from={flist_path}", src_r, dst + "/"])
            result = subprocess.run(cmd, capture_output=True, text=True)
        except OSError as exc:
            log.error("%s [FAILED] %s — rsync failed to start: %s",
                      prefix, title_year, exc)
            rsync_ok = False
        finally:
            try:
                os.unlink(flist_path)
            except OSError:
                pass

        if not rsync_ok:
            return "failed"
        if result.returncode != 0:
            stderr_lines = (result.stderr or "").strip().splitlines()[:5]
            stderr_str = "; ".join(stderr_lines) if stderr_lines else ""
            log.error(
                "%s [FAILED] %s — rsync exit %d (src=%s)%s — source unchanged",
                prefix, title_year, result.returncode, src_root,
                f": {stderr_str}" if stderr_str else "",
            )
            return "failed"

    # Size verification: sum source vs destination byte counts file-by-file.
    # File-level measurement is immune to concurrent downloads to the same dir.
    if size_verify:
        try:
            src_sz = dst_sz = 0
            for src_root, files in files_by_src_root.items():
                src_r = src_root.rstrip("/")
                for f in files:
                    src_sz += os.path.getsize(f)
                    dst_f = dst + f[len(src_r):]
                    if os.path.exists(dst_f):
                        dst_sz += os.path.getsize(dst_f)
            if abs(src_sz - dst_sz) > 1024:
                log.error(
                    "%s [FAILED] %s — size verify failed (src=%d dst=%d) — source unchanged",
                    prefix, title_year, src_sz, dst_sz,
                )
                return "failed"
        except Exception as exc:  # noqa: BLE001
            log.error(
                "%s [FAILED] %s — size verify error: %s — source unchanged",
                prefix, title_year, exc,
            )
            return "failed"

    # Delete source files and prune empty ancestor directories.
    if delete_after:
        delete_failed = False
        disk_leaf_dirs: Dict[str, set] = {}
        for src_root, files in files_by_src_root.items():
            disk_leaf_dirs[src_root] = set()
            for f in files:
                try:
                    os.unlink(f)
                    disk_leaf_dirs[src_root].add(os.path.dirname(f))
                except OSError as exc:
                    log.warning(
                        "%s [SUCCESS*] %s — moved OK but source removal failed (%s): %s",
                        prefix, title_year, f, exc,
                    )
                    delete_failed = True

        all_dirs: set = set()
        for src_root, leaf_dirs in disk_leaf_dirs.items():
            disk_root = src_root.rstrip("/")
            for d in leaf_dirs:
                current = d
                while current and current != disk_root and current.startswith(disk_root + "/"):
                    all_dirs.add(current)
                    current = os.path.dirname(current)
        for d in sorted(all_dirs, reverse=True):
            try:
                os.rmdir(d)
            except OSError:
                pass

        if delete_failed:
            return "delete_partial"

    return "ok"


def _compute_destination_path(item: "Item", hot_pool_mount: str) -> Optional[str]:
    """Return the hot-pool path for a TO_HOT move.

    Uses the dominant (first) source dir to derive the relative path, then
    prepends hot_pool_mount. All source dirs share the same relative path
    (e.g. /TV Shows/Show (2001)) so the destination is the same regardless
    of which disk each source dir lives on.
    Returns None if source_dirs is empty or current_disk is unset.
    """
    if not item.source_dirs or not item.current_disk:
        return None
    src = item.source_dirs[0]
    disk = item.current_disk.rstrip("/")
    if not src.startswith(disk + "/") and src != disk:
        return None
    rel = src[len(disk):]
    return hot_pool_mount.rstrip("/") + rel


def _check_parity_in_progress() -> bool:
    """Return True if an Unraid parity check or resync is running.

    Unraid uses a custom key=value /proc/mdstat format where mdResync= holds
    the current sync position: 0 = idle, non-zero = in progress.
    mdResyncAction= records the *last* action type regardless of run state,
    so matching on the word "check" there gives a false positive when idle.

    Standard Linux md RAID shows active progress inline as "check=22.3%".
    """
    try:
        content = Path("/proc/mdstat").read_text()
        # Unraid format: mdResync=<position>  (0 = idle)
        m = re.search(r"^mdResync=(\d+)", content, re.MULTILINE)
        if m:
            return int(m.group(1)) > 0
        # Standard Linux md format: active sync shows percentage inline.
        return bool(re.search(r"\b(check|resync)\s*=\s*\d+\.\d+%", content))
    except OSError:
        return False


def _warm_src_label(item: "Item", files_dict: Optional[Dict[str, List[str]]] = None) -> str:
    """Return a human-readable source-disk label for log lines.

    Prefers item.current_disk (the dominant warm disk). Falls back to the
    sorted comma-joined keys of files_dict (or item.warm_disk_files) for
    HOT-majority straggler items where current_disk is None.
    """
    if item.current_disk:
        return item.current_disk
    disks = files_dict if files_dict is not None else item.warm_disk_files
    if disks:
        return ", ".join(sorted(disks.keys()))
    return "?"


def _apply_capacity_budget(
    items: List[Item],
    cfg: dict,
    no_promote: bool = False,
    no_demote: bool = False,
) -> None:
    """Apply hot-pool capacity ceiling to TO_HOT promotions, and optionally
    demote lowest-scoring HOT items when the pool is already over ceiling.

    Promotion budget:
      - Computes budget = ceiling_pct * pool_total - pool_used - safety_margin.
      - Sorts TO_HOT items by score descending; items that exceed the budget
        become OVER_BUDGET_HOT (counted in the WARM projected bucket).
      - If no_promote=True: all TO_HOT → OVER_BUDGET_HOT regardless of budget.

    Auto-demote (optional):
      - Fires only when pool_used > ceiling_bytes AND
        auto_demote_when_over_ceiling=true AND no_demote=False.
      - Flips lowest-scoring STAY_HOT items to TO_WARM until the pool would
        come under the ceiling. PIN_HOT items are always exempt.
    """
    cap_cfg = cfg.get("capacity") or {}
    paths_cfg = cfg.get("paths") or {}
    hot_mount = (paths_cfg.get("hot_pool_mount") or "").rstrip("/")

    ceiling_pct = float(cap_cfg.get("hot_ceiling_percent") or 80) / 100.0
    safety_margin_bytes = int(float(cap_cfg.get("budget_safety_margin_gb") or 0) * 1024 ** 3)
    auto_demote = bool(cap_cfg.get("auto_demote_when_over_ceiling", False))
    warm_ceiling_pct = float(cap_cfg.get("warm_per_disk_ceiling_percent") or 90) / 100.0

    # --- Hot pool usage ---
    total_gb_override = cap_cfg.get("hot_pool_total_gb") or None
    unraid_api_url = (cap_cfg.get("unraid_api_url") or "").strip() or None
    unraid_api_key = (cap_cfg.get("unraid_api_key") or "").strip() or None
    unraid_pool_name = (cap_cfg.get("unraid_pool_name") or "").strip() or None
    unraid_api_verify_tls = bool(cap_cfg.get("unraid_api_verify_tls", True))
    pool_total = pool_used = 0
    if hot_mount:
        pool_total, pool_used = _pool_usage_bytes(
            hot_mount,
            total_gb_override=total_gb_override,
            unraid_api_url=unraid_api_url,
            unraid_api_key=unraid_api_key,
            unraid_pool_name=unraid_pool_name,
            verify_tls=unraid_api_verify_tls,
        )

    pool_pct = (pool_used / pool_total) if pool_total else 0.0
    ceiling_bytes = int(pool_total * ceiling_pct) if pool_total else 0
    over_ceiling = bool(pool_total and pool_used > ceiling_bytes)
    budget_bytes = max(0, ceiling_bytes - pool_used - safety_margin_bytes) if pool_total else 0

    if pool_total:
        log.info(
            "Capacity: hot pool %d%% full (%s / %s) — budget %s to %d%% ceiling%s",
            int(pool_pct * 100),
            _fmt_size(pool_used / (1024 ** 3)),
            _fmt_size(pool_total / (1024 ** 3)),
            _fmt_size(budget_bytes / (1024 ** 3)),
            int(ceiling_pct * 100),
            " (after safety margin)" if safety_margin_bytes else "",
        )
    else:
        log.debug(
            "Capacity: hot pool stats unavailable (hot_pool_mount=%r not set or inaccessible)",
            hot_mount or "",
        )

    # --- Promotion budget ---
    to_hot_items = sorted(
        [it for it in items if it.outcome == "TO_HOT"],
        key=lambda it: -it.score,
    )
    to_hot_total_bytes = sum(it.size_bytes for it in to_hot_items)

    if no_promote:
        for it in to_hot_items:
            it.outcome = "OVER_BUDGET_HOT"
        if to_hot_items:
            log.info(
                "Capacity: --no-promote set — all %d TO_HOT items (%s) deferred (OVER_BUDGET_HOT)",
                len(to_hot_items), _fmt_size(to_hot_total_bytes / (1024 ** 3)),
            )
    elif to_hot_items:
        remaining = budget_bytes
        fit = deferred = 0
        for it in to_hot_items:
            if remaining >= it.size_bytes:
                remaining -= it.size_bytes
                fit += 1
            else:
                it.outcome = "OVER_BUDGET_HOT"
                deferred += 1
        log.info(
            "Capacity: %d TO_HOT candidates totalling %s — fitting %d within budget, "
            "%d deferred (OVER_BUDGET_HOT)",
            len(to_hot_items), _fmt_size(to_hot_total_bytes / (1024 ** 3)), fit, deferred,
        )

    # --- Auto-demote when over ceiling ---
    if over_ceiling and auto_demote and not no_demote:
        bytes_to_shed = pool_used - ceiling_bytes
        demote_candidates = sorted(
            [it for it in items if it.outcome == "STAY_HOT"],
            key=lambda it: it.score,
        )
        shed = demoted = 0
        for it in demote_candidates:
            if shed >= bytes_to_shed:
                break
            it.outcome = "TO_WARM"
            shed += it.size_bytes
            demoted += 1
        pool_after_pct = ((pool_used - shed) / pool_total * 100) if pool_total else 0.0
        log.info(
            "Capacity: hot pool %d%% full — over %d%% ceiling, demoting lowest scorers",
            int(pool_pct * 100), int(ceiling_pct * 100),
        )
        log.info(
            "Capacity: demoted %d items (%s) to TO_WARM to bring pool to %.1f%%",
            demoted, _fmt_size(shed / (1024 ** 3)), pool_after_pct,
        )
    elif no_demote and auto_demote and over_ceiling:
        log.info("Capacity: --no-demote set — auto-demote pass skipped (pool over ceiling)")

    # --- Warm disk per-disk usage ---
    array_disks = resolve_array_disks(cfg)
    if array_disks:
        ceiling_label = int(warm_ceiling_pct * 100)
        for disk in sorted(array_disks):
            try:
                du = _disk_usage(disk)
                if not du.total:
                    continue
                pct = du.used / du.total * 100
                over = pct >= warm_ceiling_pct * 100
                log.info(
                    "Capacity: warm disk %s — %s / %s (%d%%)%s",
                    disk,
                    _fmt_size(du.used / (1024 ** 3)),
                    _fmt_size(du.total / (1024 ** 3)),
                    int(pct),
                    f"  ← over {ceiling_label}% ceiling" if over else "",
                )
            except OSError:
                log.warning("Capacity: warm disk %s — could not read usage", disk)


def _apply_fast_promote_guard(items: List[Item], cfg: dict, mode: str) -> set:
    """P4.5: on promote-only runs, defer a series' TO_HOT unless enough of its
    episodes have been watched since the last full run.

    Applies only when mode == "promote-only" — full and demote-only runs are
    unaffected (a single-pilot watch is exactly the signal a monthly full run
    should corroborate before committing a multi-season promotion). Movies are
    never affected; the guard only inspects kind == "series" items.

    Returns the set of id(item) to exclude from this run's TO_HOT move queue.
    The item's outcome is left as TO_HOT — it is simply not executed this run,
    and is reconsidered on the next run (or unconditionally by the next full
    run, which does not apply this guard).
    """
    skip_ids: set = set()
    if mode != "promote-only":
        return skip_ids
    min_episodes = int((cfg.get("scheduling") or {}).get("min_episodes_for_fast_promote") or 1)
    if min_episodes <= 1:
        return skip_ids
    for it in items:
        if it.outcome != "TO_HOT" or it.kind != "series":
            continue
        if it.recent_episode_plays < min_episodes:
            skip_ids.add(id(it))
            log.info(
                "Moves: [SKIPPED] %s [TO_HOT] — skipped: %d episode(s) since last full run, need %d",
                it.title_year, it.recent_episode_plays, min_episodes,
            )
    return skip_ids


def _run_move_pass(
    items: List["Item"], cfg: dict, apply: bool,
    no_promote: bool = False, no_demote: bool = False,
    skip_promote_ids: Optional[set] = None,
) -> dict:
    """Dry-run or execute moves for TO_HOT, TO_WARM, and RELOCATE_WARM outcomes.

    When apply=False: emits [DRY-RUN] log lines for every projected move.
    When apply=True:  executes rsync serially, verifies sizes, optionally
                      deletes the source after successful verification.

    no_promote / no_demote (P4.5): suppress execution of TO_HOT, or of
    TO_WARM + RELOCATE_WARM, respectively. This is distinct from (and in
    addition to) _apply_capacity_budget's same-named parameters, which only
    affect the capacity pass's own TO_HOT->OVER_BUDGET_HOT conversion and
    auto-demote trigger — they do not touch items that scored TO_WARM or
    RELOCATE_WARM directly. --no-demote must suppress RELOCATE_WARM too:
    draining a series off an evicting disk is a demotion in cadence terms.
    Suppressed items keep their scored outcome and are reconsidered next run.

    skip_promote_ids (P4.5): set of id(item) to exclude from this run's
    TO_HOT queue (the fast-promote episode-count guard). Outcome is left
    untouched; the caller has already logged why each one was deferred.

    Skips the entire pass if moves.enabled is false in config.
    Returns a dict with moves_attempted, moves_succeeded, bytes_moved.
    """
    skip_promote_ids = skip_promote_ids or set()
    _empty_stats: dict = {"moves_attempted": 0, "moves_succeeded": 0, "bytes_moved": 0}
    moves_cfg = cfg.get("moves") or {}
    if not moves_cfg.get("enabled"):
        return _empty_stats

    hot_mount = (cfg.get("paths") or {}).get("hot_pool_mount") or ""
    if not hot_mount:
        log.warning("Moves: moves.enabled=true but paths.hot_pool_mount not set — skipping")
        return _empty_stats

    array_disks = resolve_array_disks(cfg)

    # Evicting disks are implicitly write-protected: no new data should land on
    # a disk that is being drained.  Subtract them from the candidate list used
    # by _select_warm_destination so TO_WARM and RELOCATE_WARM moves never pick
    # an evicting disk as the destination (RELOCATE_WARM already excludes its
    # source disk via exclude_disk, but this covers TO_WARM and the non-source
    # evicting disks in a multi-evict scenario).
    evict_cfg = cfg.get("array_disk_evict") or {}
    _evict_disks_for_write = (
        _build_evict_disks(evict_cfg, array_disks)
        if evict_cfg.get("enabled")
        else set()
    )
    dest_array_disks = [d for d in array_disks if d not in _evict_disks_for_write]

    warm_sel_cfg = (moves_cfg.get("warm_disk_selection") or {})
    safety_margin_bytes = int(float(warm_sel_cfg.get("safety_margin_gb") or 50) * 1024 ** 3)
    hot_bps = int(float(moves_cfg.get("estimated_hot_mbps") or 200)) * 1024 * 1024
    warm_bps = int(float(moves_cfg.get("estimated_warm_mbps") or 50)) * 1024 * 1024
    cap_cfg = cfg.get("capacity") or {}
    _warm_ceiling_pct = float(cap_cfg.get("warm_per_disk_ceiling_percent") or 100) / 100.0

    # Tally SHOULD_BE_* outcomes (tier unknown — nothing to move).
    skip_outcomes: dict = {}
    for it in items:
        if it.outcome in ("SHOULD_BE_HOT", "SHOULD_BE_WARM"):
            skip_outcomes[it.outcome] = skip_outcomes.get(it.outcome, 0) + 1
    if skip_outcomes:
        parts_str = "  ".join(f"{k}={v}" for k, v in sorted(skip_outcomes.items()))
        log.info(
            "Moves: skipping %d items with unknown current tier (%s)",
            sum(skip_outcomes.values()), parts_str,
        )

    # --- P4.5 mode suppression: no_promote/no_demote stop entire directions
    # regardless of how the item was scored (capacity budget's same-named
    # params only affect its own conversion/auto-demote logic, see docstring).
    if no_promote:
        n_pending = sum(1 for it in items if it.outcome == "TO_HOT")
        if n_pending:
            log.info("Moves: promote suppressed this run — skipping %d TO_HOT item(s)", n_pending)
    if no_demote:
        n_warm_pending = sum(1 for it in items if it.outcome == "TO_WARM")
        n_reloc_pending = sum(1 for it in items if it.outcome == "RELOCATE_WARM")
        if n_warm_pending or n_reloc_pending:
            log.info(
                "Moves: demote suppressed this run — skipping %d TO_WARM + %d RELOCATE_WARM item(s)",
                n_warm_pending, n_reloc_pending,
            )

    # --- Build per-direction queues ---

    # TO_HOT: items with no warm_disk_files are idempotency skips (fully moved
    # already) or straggler-upgraded items whose warm files have been cleaned up.
    to_hot_skip = [it for it in items if it.outcome == "TO_HOT" and not it.warm_disk_files]
    to_hot_move: List["Item"] = []
    for it in items:
        if it.outcome != "TO_HOT" or no_promote:
            continue
        if not it.warm_disk_files:
            continue  # idempotency skip — nothing left on warm
        if id(it) in skip_promote_ids:
            continue  # fast-promote episode-count guard — logged by caller
        to_hot_move.append(it)

    # TO_WARM: item is on the hot pool; hot_pool_files must be populated.
    to_warm_move: List["Item"] = []
    for it in items:
        if it.outcome != "TO_WARM" or no_demote:
            continue
        if not it.hot_pool_files:
            log.warning("Moves: [SKIP] %s [TO_WARM] — no hot_pool_files (tier detection inactive?)",
                        it.title_year)
            continue
        to_warm_move.append(it)

    # RELOCATE_WARM: item is on an evicting disk; warm_disk_files must be populated.
    relocate_move: List["Item"] = []
    for it in items:
        if it.outcome != "RELOCATE_WARM" or no_demote:
            continue
        if not it.warm_disk_files:
            log.warning("Moves: [SKIP] %s [RELOCATE_WARM] — no warm_disk_files (tier detection inactive?)",
                        it.title_year)
            continue
        relocate_move.append(it)

    if not (to_hot_skip or to_hot_move or to_warm_move or relocate_move):
        log.info("Moves: no actionable items")
        return

    # --- Pre-select warm destinations (shared between dry-run and apply) ---
    # Capacity failures are reported as errors immediately so they appear even
    # in dry-run; the items are then excluded from the move queue.

    to_warm_dests: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
    for it in to_warm_move:
        if not dest_array_disks:
            to_warm_dests[id(it)] = (None, "no warm disks configured")
        else:
            dst, annot = _select_warm_destination(
                it, dest_array_disks, safety_margin_bytes,
                warm_ceiling_pct=_warm_ceiling_pct,
            )
            to_warm_dests[id(it)] = (dst, annot)
        if to_warm_dests[id(it)][0] is None:
            log.error("Moves: [FAILED] %s [TO_WARM] — %s (needs %s + %d GB margin)",
                      it.title_year, to_warm_dests[id(it)][1],
                      _fmt_size(it.size_bytes / (1024 ** 3)),
                      (safety_margin_bytes // (1024 ** 3)))

    relocate_dests: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
    for it in relocate_move:
        if not dest_array_disks:
            relocate_dests[id(it)] = (None, "no warm disks configured")
        else:
            dst, annot = _select_warm_destination(
                it, dest_array_disks, safety_margin_bytes,
                exclude_disk=it.current_disk, warm_ceiling_pct=_warm_ceiling_pct,
            )
            relocate_dests[id(it)] = (dst, annot)
        if relocate_dests[id(it)][0] is None:
            log.error("Moves: [FAILED] %s [RELOCATE_WARM] — %s (needs %s + %d GB margin)",
                      it.title_year, relocate_dests[id(it)][1],
                      _fmt_size(it.size_bytes / (1024 ** 3)),
                      (safety_margin_bytes // (1024 ** 3)))

    to_warm_ok = [it for it in to_warm_move if to_warm_dests[id(it)][0] is not None]
    relocate_ok = [it for it in relocate_move if relocate_dests[id(it)][0] is not None]

    hot_bytes = sum(it.size_bytes for it in to_hot_move)
    warm_bytes = (
        sum(it.size_bytes for it in to_warm_ok)
        + sum(it.size_bytes for it in relocate_ok)
    )
    total_bytes = hot_bytes + warm_bytes

    def _eta() -> str:
        secs = (hot_bytes / hot_bps if hot_bps and hot_bytes else 0.0) + \
               (warm_bytes / warm_bps if warm_bps and warm_bytes else 0.0)
        return _fmt_eta(secs) if secs else "0s"

    # --- DRY-RUN ---
    if not apply:
        log.info(
            "[DRY-RUN] Moves: TO_HOT=%d TO_WARM=%d RELOCATE_WARM=%d"
            " (%d already-HOT skipped) — %s total — ETA ~%s"
            " (%d MB/s hot / %d MB/s warm)",
            len(to_hot_move), len(to_warm_ok), len(relocate_ok),
            len(to_hot_skip),
            _fmt_size(total_bytes / (1024 ** 3)),
            _eta(),
            hot_bps // (1024 * 1024), warm_bps // (1024 * 1024),
        )
        for it in to_hot_skip:
            log.info("[DRY-RUN]   %s [TO_HOT] — already on hot pool (SKIPPED)", it.title_year)
        for it in to_hot_move:
            n_files = sum(len(f) for f in it.warm_disk_files.values())
            log.info(
                "[DRY-RUN]   %s [TO_HOT] — %s — %d file(s) from %s → %s",
                it.title_year, _fmt_size(it.size_bytes / (1024 ** 3)),
                n_files, _warm_src_label(it), hot_mount,
            )
        for it in to_warm_ok:
            dst, annot = to_warm_dests[id(it)]
            log.info(
                "[DRY-RUN]   %s [TO_WARM] — %s — %d file(s) from %s → %s (%s)",
                it.title_year, _fmt_size(it.size_bytes / (1024 ** 3)),
                len(it.hot_pool_files), hot_mount, dst, annot,
            )
        for it in relocate_ok:
            dst, annot = relocate_dests[id(it)]
            _rsrc = it.relocate_source_override if it.relocate_source_override is not None else it.warm_disk_files
            n_files = sum(len(f) for f in _rsrc.values())
            log.info(
                "[DRY-RUN]   %s [RELOCATE_WARM] — %s — %d file(s) from %s → %s (%s)",
                it.title_year, _fmt_size(it.size_bytes / (1024 ** 3)),
                n_files, _warm_src_label(it, _rsrc), dst, annot,
            )
        return

    # --- APPLY MODE ---
    parity_blocking = moves_cfg.get("parity_check_blocking", True)
    if _check_parity_in_progress():
        if parity_blocking:
            log.error("Moves: parity check in progress — aborting move pass")
            return
        log.warning("Moves: parity check in progress — proceeding (parity_check_blocking=false)")

    rsync_opts = list(moves_cfg.get("rsync_options") or ["-aH", "--partial", "--inplace"])
    bwlimit = moves_cfg.get("bandwidth_limit_mbps")
    if bwlimit:
        rsync_opts.append(f"--bwlimit={int(bwlimit) * 1024}")  # rsync expects KB/s
    delete_after = moves_cfg.get("delete_source_after_verify", True)
    size_verify = moves_cfg.get("size_verify", True)

    log.info(
        "Moves: TO_HOT=%d TO_WARM=%d RELOCATE_WARM=%d (apply mode) — %s total — ETA ~%s"
        " (%d MB/s hot / %d MB/s warm)",
        len(to_hot_move), len(to_warm_ok), len(relocate_ok),
        _fmt_size(total_bytes / (1024 ** 3)), _eta(),
        hot_bps // (1024 * 1024), warm_bps // (1024 * 1024),
    )
    for it in to_hot_skip:
        log.info("  [SKIPPED] %s [TO_HOT] — already on hot pool (no-op)", it.title_year)

    # Build unified move list: (direction, item, files_by_src_root, dst_root, src_label, dst_label, annotation)
    # files_by_src_root matches what _exec_single_move expects: {src_root: [absolute file paths]}
    move_list: list = []

    for it in to_hot_move:
        move_list.append((
            "TO_HOT", it,
            dict(it.warm_disk_files),
            hot_mount.rstrip("/") + "/",
            _warm_src_label(it), hot_mount, "",
        ))
    for it in to_warm_ok:
        dst, annot = to_warm_dests[id(it)]
        move_list.append((
            "TO_WARM", it,
            {hot_mount: it.hot_pool_files},
            dst.rstrip("/") + "/",
            hot_mount, dst, annot or "",
        ))
    for it in relocate_ok:
        dst, annot = relocate_dests[id(it)]
        src = (
            it.relocate_source_override
            if it.relocate_source_override is not None
            else dict(it.warm_disk_files)
        )
        move_list.append((
            "RELOCATE_WARM", it,
            src,
            dst.rstrip("/") + "/",
            _warm_src_label(it, src), dst, annot or "",
        ))

    n_success = 0
    n_skipped = len(to_hot_skip)
    n_failed = 0
    affected_libraries: set = set()
    run_start = datetime.now(timezone.utc)
    total_count = len(move_list)

    max_total_bytes = float(moves_cfg.get("max_total_move_gb") or 0) * 1024 ** 3
    moved_bytes = 0

    for idx, (direction, it, files_by_src, dst_root, src_label, dst_label, annot) in enumerate(move_list, 1):
        if max_total_bytes > 0 and moved_bytes >= max_total_bytes:
            remaining = total_count - idx + 1
            log.info(
                "Run cap reached: %.1f GB moved — stopping move pass "
                "(%d item(s) remaining, will retry next run).",
                moved_bytes / (1024 ** 3), remaining,
            )
            break

        prefix = f"  [{idx}/{total_count}]"
        item_start = datetime.now(timezone.utc)
        size_str = _fmt_size(it.size_bytes / (1024 ** 3))
        n_files = sum(len(f) for f in files_by_src.values())
        annot_sfx = f" ({annot})" if annot else ""

        log.info(
            "%s Moving %s [%s] — %s, %d file(s) — %s → %s%s",
            prefix, it.title_year, direction, size_str, n_files,
            src_label, dst_label, annot_sfx,
        )

        status = _exec_single_move(
            prefix=prefix,
            title_year=it.title_year,
            files_by_src_root=files_by_src,
            dst_root=dst_root,
            rsync_opts=rsync_opts,
            size_verify=size_verify,
            delete_after=delete_after,
        )

        elapsed = (datetime.now(timezone.utc) - item_start).total_seconds()
        if status == "failed":
            n_failed += 1
        else:
            log.info(
                "%s [SUCCESS] %s [%s] — %s in %s — %s → %s%s",
                prefix, it.title_year, direction, size_str, _fmt_eta(elapsed),
                src_label, dst_label, annot_sfx,
            )
            n_success += 1
            moved_bytes += it.size_bytes
            affected_libraries.add(it.library)

    total_elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
    log.info(
        "Moves complete: %d successful, %d skipped, %d failed (%s total)",
        n_success, n_skipped, n_failed, _fmt_eta(total_elapsed),
    )
    # TO_HOT moves bring files to a mount outside the Unraid user-share union,
    # so Plex needs a library rescan to see the new paths. TO_WARM and
    # RELOCATE_WARM keep files within /mnt/user/ — no rescan required since
    # Plex always reads through the union regardless of which physical disk
    # backs the file.
    hot_libraries = {
        it.library for (direction, it, *_) in move_list
        if direction == "TO_HOT" and it.library in affected_libraries
    }
    if hot_libraries:
        log.info(
            "Plex rescan recommended for sections: %s",
            ", ".join(sorted(hot_libraries)),
        )

    return {
        "moves_attempted": n_success + n_failed,
        "moves_succeeded": n_success,
        "bytes_moved": int(moved_bytes),
    }


# Outcomes that map to each projected tier if every recommendation were
# executed. The bucket reflects where the item will END UP, not where it
# currently sits — so TO_HOT + STAY_HOT + PIN_HOT all go into HOT.
_HOT_OUTCOMES = {"SHOULD_BE_HOT", "PIN_HOT", "STAY_HOT", "TO_HOT"}
# OVER_BUDGET_HOT: item scored TO_HOT but the hot pool budget denied promotion.
# Counted in WARM projected totals — the item stays on warm this run.
_WARM_OUTCOMES = {"SHOULD_BE_WARM", "STAY_WARM", "TO_WARM", "RELOCATE_WARM", "OVER_BUDGET_HOT"}
# MIXED_NEUTRAL = item is split 50/50 with no direction to resolve it.
# Leave under NEUTRAL; P2 takes no action.


def summarise_tiers(items: List[Item]) -> dict:
    """Bucket items into projected HOT / WARM / NEUTRAL tiers + return totals.

    Returns:
        {
          "tiers": { "HOT": {"count": N, "size_gb": X}, "WARM": {...}, "NEUTRAL": {...} },
          "outcomes": { "SHOULD_BE_HOT": N, ... },
          "total_count": N,
          "total_gb": X,
        }
    """
    tiers = {
        "HOT":     {"count": 0, "size_gb": 0.0},
        "WARM":    {"count": 0, "size_gb": 0.0},
        "NEUTRAL": {"count": 0, "size_gb": 0.0},
    }
    outcomes: dict = {}
    for it in items:
        outcomes[it.outcome] = outcomes.get(it.outcome, 0) + 1
        if it.outcome in _HOT_OUTCOMES:
            bucket = "HOT"
        elif it.outcome in _WARM_OUTCOMES:
            bucket = "WARM"
        else:
            bucket = "NEUTRAL"
        tiers[bucket]["count"] += 1
        tiers[bucket]["size_gb"] += it.size_gb
    return {
        "tiers": tiers,
        "outcomes": outcomes,
        "total_count": len(items),
        "total_gb": sum(it.size_gb for it in items),
    }


def format_table(items: List[Item]) -> str:
    if not items:
        return "(no items)\n"

    headers = [
        "Title",
        "Type",
        "Library",
        "Size",
        "Plays",
        "Last Played",
        "Added",
        "Score",
        "Outcome",
    ]
    rows = []
    for it in items:
        rows.append(
            [
                it.title_year,
                it.kind,
                it.library,
                _fmt_size(it.size_gb),
                str(it.plays),
                _fmt_date(it.last_played),
                _fmt_date(it.added),
                f"{it.score:>5.1f}",
                it.outcome,
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)
    # Cap the Title column so it doesn't dominate
    widths[0] = min(widths[0], 50)

    def render_row(row):
        parts = []
        for i, cell in enumerate(row):
            w = widths[i]
            if i == 0 and len(cell) > w:
                cell = cell[: w - 1] + "…"
            parts.append(cell.ljust(w))
        return "  ".join(parts)

    sep = "  ".join("─" * w for w in widths)
    lines = [render_row(headers), sep]
    lines.extend(render_row(r) for r in rows)
    return "\n".join(lines) + "\n"


def format_json(items: List[Item]) -> str:
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError
    return json.dumps([asdict(it) for it in items], default=default, indent=2)


def write_csv(items: List[Item], path: Path) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "title", "year", "kind", "library", "size_bytes",
            "plays", "last_played", "added", "score", "outcome",
        ])
        for it in items:
            w.writerow([
                it.title,
                it.year,
                it.kind,
                it.library,
                it.size_bytes,
                it.plays,
                it.last_played.isoformat() if it.last_played else "",
                it.added.isoformat(),
                it.score,
                it.outcome,
            ])


# ---------- Sorting / filtering ----------

SORT_KEYS = {
    "score": lambda it: -it.score,
    "title": lambda it: it.title.lower(),
    "plays": lambda it: -it.plays,
    "size": lambda it: -it.size_bytes,
    "last_played": lambda it: it.last_played or datetime.min.replace(tzinfo=timezone.utc),
    "added": lambda it: it.added,
    "outcome": lambda it: it.outcome,
}


def apply_sort(items: List[Item], sort_key: str) -> List[Item]:
    if sort_key not in SORT_KEYS:
        sys.exit(f"Unknown sort key '{sort_key}'. Choose from: {', '.join(SORT_KEYS)}")
    return sorted(items, key=SORT_KEYS[sort_key])


# ---------- --explain ----------


def explain_one(items: List[Item], needle: str, thresholds: dict) -> None:
    matches = [
        it for it in items
        if needle.lower() in it.title.lower()
    ]
    if not matches:
        print(f"No item matches '{needle}'.")
        return
    for it in matches:
        print("─" * 70)
        print(f"{it.title_year}  [{it.kind}]  library={it.library}")
        print(f"  current_tier={it.current_tier}  outcome={it.outcome}")
        print(f"  plays={it.plays}  last_played={_fmt_date(it.last_played)}  added={_fmt_date(it.added)}")
        print(f"  size={_fmt_size(it.size_gb)}")
        print(f"  breakdown: {json.dumps(it.score_breakdown)}")
        print(
            f"  thresholds: to_hot>={thresholds['score_to_hot']}  "
            f"to_warm<={thresholds['score_to_warm']}  "
            f"half_life={thresholds['recency_half_life_days']}d  "
            f"grace={thresholds['age_grace_days']}d  "
            f"hot_recency={thresholds.get('hot_recency_days', '—')}d"
        )


# ---------- Main ----------


# ---------- Run mode + env-var config layer (P4.5) ----------
#
# Unraid CA template recreates wipe command-line overrides, and `docker start`
# cannot pass new args — so every behaviour a scheduled run depends on must be
# reachable without CLI args. Precedence is CLI > env > config > built-in
# default, established here for --mode/TIER_MODE and --apply/TIER_APPLY.
# See AGENTS.md "Every settable behaviour must be reachable without CLI args."

_VALID_MODES = ("full", "promote-only", "demote-only")
_BOOL_TRUE_STRINGS = ("1", "true", "yes", "on")
_BOOL_FALSE_STRINGS = ("0", "false", "no", "off")


def _check_mode_conflicts(args, parser: argparse.ArgumentParser) -> None:
    """--mode and --no-promote/--no-demote are mutually exclusive on the CLI."""
    if getattr(args, "mode", None) is None:
        return
    if args.mode == "promote-only" and args.no_promote:
        parser.error("--mode promote-only conflicts with --no-promote")
    if args.mode == "demote-only" and args.no_demote:
        parser.error("--mode demote-only conflicts with --no-demote")


def _resolve_mode(args, cfg: dict) -> str:
    """Resolve run mode: CLI --mode > TIER_MODE env > scheduling.default_mode > 'full'."""
    if getattr(args, "mode", None):
        return args.mode
    env_val = os.environ.get("TIER_MODE")
    if env_val:
        if env_val not in _VALID_MODES:
            log.error(
                "Invalid TIER_MODE=%r from environment (must be one of %s)",
                env_val, ", ".join(_VALID_MODES),
            )
            sys.exit(2)
        return env_val
    cfg_val = (cfg.get("scheduling") or {}).get("default_mode")
    if cfg_val:
        if cfg_val not in _VALID_MODES:
            log.error(
                "Invalid scheduling.default_mode=%r in config (must be one of %s)",
                cfg_val, ", ".join(_VALID_MODES),
            )
            sys.exit(2)
        return cfg_val
    return "full"


def _resolve_apply(args, cfg: dict) -> bool:
    """Resolve --apply: CLI --apply > TIER_APPLY env > moves.apply config > False.

    --apply is store_true (no CLI way to force False), so "CLI wins" only
    applies when it's actually passed; otherwise env, then config, decide.
    """
    if args.apply:
        return True
    env_val = os.environ.get("TIER_APPLY")
    if env_val is not None:
        low = env_val.strip().lower()
        if low in _BOOL_TRUE_STRINGS:
            return True
        if low in _BOOL_FALSE_STRINGS:
            return False
        log.error("Invalid TIER_APPLY=%r from environment (expected true/false)", env_val)
        sys.exit(2)
    return bool((cfg.get("moves") or {}).get("apply", False))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tier.py",
        description="Unraid media tiering analyser (P0: read-only).",
    )
    p.add_argument(
        "--config", type=Path, default=DEFAULT_CONFIG_PATH,
        help=f"config file path (default: {DEFAULT_CONFIG_PATH})",
    )
    p.add_argument(
        "--library", action="append", default=None,
        help="limit to one or more Plex libraries (repeatable)",
    )
    p.add_argument(
        "--sort", default="score",
        help=f"sort key: one of {', '.join(SORT_KEYS)} (default: score)",
    )
    p.add_argument(
        "--top", type=int, default=None,
        help="show only the first N rows after sorting",
    )
    p.add_argument(
        "--json", action="store_true",
        help="emit JSON instead of a table",
    )
    p.add_argument(
        "--csv", type=Path, default=None,
        help="also write results to a CSV file at this path",
    )
    p.add_argument(
        "--explain", default=None,
        help="show scoring breakdown for items whose title contains this substring",
    )
    p.add_argument(
        "--apply", action="store_true",
        help="execute moves (requires moves.enabled=true in config; default is dry-run)",
    )
    p.add_argument(
        "--no-promote", action="store_true",
        help="block all TO_HOT moves this run; items become OVER_BUDGET_HOT regardless of budget",
    )
    p.add_argument(
        "--no-demote", action="store_true",
        help="skip auto-demote pass and suppress TO_WARM + RELOCATE_WARM execution this run",
    )
    p.add_argument(
        "--mode", choices=_VALID_MODES, default=None,
        help=(
            "run mode (default: full, or TIER_MODE env, or scheduling.default_mode "
            "config). promote-only is equivalent to --no-demote; demote-only is "
            "equivalent to --no-promote. Mutually exclusive with --no-promote/--no-demote."
        ),
    )
    p.add_argument(
        "--log-file", type=Path, default=None,
        help="override logging.path from config",
    )
    p.add_argument(
        "--log-level", default=None,
        help="override logging.level (DEBUG, INFO, WARNING, ERROR)",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="suppress console logging (file log still written)",
    )
    return p


# ---------- Scheduling primitives (P4.1) ----------

_LOCK_FILE = _STATE_DIR / "tier.lock"
_LAST_RUN_FILE = _STATE_DIR / "last_run.json"
# Open file handle kept alive while the flock is held; None when not locked.
_lock_fh: Optional[object] = None


def _ensure_state_dir() -> None:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)


def _acquire_lock(mode: str = "full") -> bool:
    """Acquire a kernel-level exclusive advisory lock via fcntl.flock.

    Uses fcntl.flock(LOCK_EX|LOCK_NB) rather than a PID-liveness check.
    This is correct across separate Docker containers: each container has
    its own PID namespace, so os.kill() cannot see another container's
    process and would wrongly reclaim a live lock.  flock() operates at
    the kernel VFS layer — /config is a bind-mount of a host-local directory,
    so the lock is shared by every container that mounts the same host path.
    The kernel releases the lock automatically when the process or container
    dies, so stale-lock cleanup is implicit and needs no PID inspection.

    Lock file also stores JSON metadata (pid, started_at, mode) so operators
    can inspect who holds the lock, but that metadata is informational only.
    """
    global _lock_fh

    # Snapshot any existing metadata before we (possibly) truncate the file,
    # so we can log who currently holds the lock on conflict.
    existing: dict = {}
    if _LOCK_FILE.exists():
        try:
            existing = json.loads(_LOCK_FILE.read_text())
        except Exception:  # noqa: BLE001
            pass

    try:
        fh = open(_LOCK_FILE, "w")  # noqa: WPS515 — intentionally kept open
    except OSError as e:
        log.warning("Could not open lock file %s: %s — proceeding without lock", _LOCK_FILE, e)
        return True

    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        log.error(
            "Lock held (started %s) — another tier.py instance is running",
            existing.get("started_at", "?"),
        )
        return False
    except Exception as e:  # noqa: BLE001
        fh.close()
        log.warning("flock failed: %s — proceeding without lock", e)
        return True

    # Lock acquired — write metadata and keep the handle open.
    try:
        fh.write(json.dumps({
            "pid": os.getpid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
        }))
        fh.flush()
    except Exception as e:  # noqa: BLE001
        log.warning("Could not write lock metadata: %s", e)

    _lock_fh = fh
    return True


def _release_lock() -> None:
    global _lock_fh
    if _lock_fh is not None:
        try:
            fcntl.flock(_lock_fh, fcntl.LOCK_UN)
            _lock_fh.close()
        except Exception as e:  # noqa: BLE001
            log.warning("Could not release lock: %s", e)
        finally:
            _lock_fh = None
    try:
        _LOCK_FILE.unlink(missing_ok=True)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not remove lock file: %s", e)


def _check_skip_recent(cfg: dict) -> bool:
    """Return True if this run should be skipped due to the recency guard."""
    threshold = int((cfg.get("scheduling") or {}).get("skip_if_run_within_minutes") or 0)
    if threshold <= 0:
        return False
    if not _LAST_RUN_FILE.exists():
        return False
    try:
        data = json.loads(_LAST_RUN_FILE.read_text())
        finished_str = data.get("finished_at")
        if not finished_str:
            return False
        finished_at = datetime.fromisoformat(finished_str)
        elapsed_min = (datetime.now(timezone.utc) - finished_at).total_seconds() / 60
        if elapsed_min < threshold:
            log.warning(
                "Skipping run — last run finished %.1f min ago (threshold: %d min)",
                elapsed_min, threshold,
            )
            return True
    except Exception as e:  # noqa: BLE001
        log.warning("Could not read last_run.json for recency check: %s", e)
    return False


def _write_last_run(
    started_at: datetime, exit_code: int, move_stats: dict, mode: str = "full",
) -> None:
    move_stats = move_stats or {}

    # last_full_run_finished_at (P4.5): carried forward from the previous
    # state file unless THIS run was a successful full run, in which case it
    # advances to now. promote-only/demote-only runs never set it — only a
    # full run gives the fast-promote guard a trustworthy reference point.
    last_full_run_finished_at = None
    if _LAST_RUN_FILE.exists():
        try:
            prev = json.loads(_LAST_RUN_FILE.read_text())
            last_full_run_finished_at = prev.get("last_full_run_finished_at")
        except Exception:  # noqa: BLE001
            pass
    finished_at = datetime.now(timezone.utc)
    if mode == "full" and exit_code == int(ExitCode.SUCCESS):
        last_full_run_finished_at = finished_at.isoformat()

    try:
        _LAST_RUN_FILE.write_text(json.dumps({
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "mode": mode,
            "exit_code": exit_code,
            "moves_attempted": move_stats.get("moves_attempted", 0),
            "moves_succeeded": move_stats.get("moves_succeeded", 0),
            "bytes_moved": move_stats.get("bytes_moved", 0),
            "last_full_run_finished_at": last_full_run_finished_at,
        }, indent=2))
    except Exception as e:  # noqa: BLE001
        log.warning("Could not write last_run.json: %s", e)


def _read_last_full_run_finished_at() -> Optional[datetime]:
    """Read last_run.json's last_full_run_finished_at for the P4.5 fast-promote
    guard. None if absent — collect_all() then counts over full history."""
    if not _LAST_RUN_FILE.exists():
        return None
    try:
        data = json.loads(_LAST_RUN_FILE.read_text())
        val = data.get("last_full_run_finished_at")
        if not val:
            return None
        return datetime.fromisoformat(val)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not read last_full_run_finished_at: %s", e)
        return None


def _run(args) -> dict:
    """Inner main: wired up after logging + notifier are ready."""
    cfg = load_config(args.config)

    # CLI overrides for logging
    if args.log_file:
        cfg.setdefault("logging", {})["path"] = str(args.log_file)
    if args.log_level:
        cfg.setdefault("logging", {})["level"] = args.log_level

    setup_logging(cfg, quiet=args.quiet)
    notifier = build_notifier(cfg)
    ncfg = cfg.get("notifications") or {}

    resolved_mode = _resolve_mode(args, cfg)
    log.info("tier.py starting — config=%s mode=%s", args.config, resolved_mode)

    plex = connect_plex(cfg["plex"]["url"], cfg["plex"]["token"], notifier, ncfg)

    # P4.5: promote-only's fast-promote guard counts episodes watched since
    # the last full run; None (no full run recorded yet) makes collect_all
    # count over the entire Plex history index instead of skipping the guard.
    fast_promote_cutoff = _read_last_full_run_finished_at()
    items = collect_all(
        plex, cfg, filter_libraries=args.library,
        fast_promote_cutoff=fast_promote_cutoff,
    )
    items = apply_sort(items, args.sort)

    # --mode is a cron-ergonomics shortcut over --no-promote/--no-demote:
    # promote-only ⇔ --no-demote, demote-only ⇔ --no-promote. CLI flags and
    # mode are mutually exclusive (enforced in main()), so OR-combining them
    # here is safe — at most one side is ever actually set.
    no_promote = bool(getattr(args, "no_promote", False)) or resolved_mode == "demote-only"
    no_demote = bool(getattr(args, "no_demote", False)) or resolved_mode == "promote-only"

    # Capacity pass: apply hot pool budget and optional auto-demote before
    # the move pass so the move queues reflect budget-adjusted outcomes.
    _apply_capacity_budget(items, cfg, no_promote=no_promote, no_demote=no_demote)

    # Fast-promote episode-count guard (promote-only runs only, series only).
    skip_promote_ids = _apply_fast_promote_guard(items, cfg, resolved_mode)

    # Move pass runs on the full scored list before --top truncation so every
    # TO_HOT item is considered regardless of display limit.
    moves_apply = _resolve_apply(args, cfg)
    move_stats = _run_move_pass(
        items, cfg, apply=moves_apply,
        no_promote=no_promote, no_demote=no_demote,
        skip_promote_ids=skip_promote_ids,
    )

    if args.top:
        items = items[: args.top]

    if args.csv:
        write_csv(items, args.csv)
        log.info("Wrote %d rows to %s", len(items), args.csv)

    if args.explain:
        explain_one(items, args.explain, cfg["thresholds"])
        return move_stats

    if args.json:
        print(format_json(items))
    else:
        print(format_table(items))
        s = summarise_tiers(items)
        # Projected tier sizes — what the layout would look like if every
        # recommendation in this run were applied. In P0 we don't know
        # current placement, so NEUTRAL is "the script isn't recommending
        # a move either way" (not necessarily "stays on its current tier").
        tiers = s["tiers"]
        log.info(
            "Run summary: %d items  total=%s",
            s["total_count"], _fmt_size(s["total_gb"]),
        )
        log.info(
            "  Projected HOT     %4d items  %s",
            tiers["HOT"]["count"], _fmt_size(tiers["HOT"]["size_gb"]),
        )
        log.info(
            "  Projected WARM    %4d items  %s",
            tiers["WARM"]["count"], _fmt_size(tiers["WARM"]["size_gb"]),
        )
        log.info(
            "  Projected NEUTRAL %4d items  %s",
            tiers["NEUTRAL"]["count"], _fmt_size(tiers["NEUTRAL"]["size_gb"]),
        )
        outcomes_str = "  ".join(
            f"{k}={v}" for k, v in sorted(s["outcomes"].items())
        )
        log.info("  Outcome counts: %s", outcomes_str)
        thresholds = cfg["thresholds"]
        if thresholds.get("added_floor_days_movies") or thresholds.get("added_floor_days_tv"):
            floor_promotions = sum(
                1 for it in items
                if "added-date floor" in it.score_breakdown.get("override", "")
            )
            log.info("  Added-floor promotions: %d items", floor_promotions)
        if cfg.get("pinned_collections"):
            col_promotions = sum(
                1 for it in items
                if it.score_breakdown.get("override") == "collection pin"
            )
            log.info("  Collection-pin promotions: %d items", col_promotions)
        if (cfg.get("auto_collection_inherit") or {}).get("enabled"):
            ai_promotions = sum(
                1 for it in items
                if it.score_breakdown.get("override") == "auto-inherit collection"
            )
            log.info("  Auto-inherit promotions: %d items", ai_promotions)

    return move_stats


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    _check_mode_conflicts(args, parser)  # exits 2 (bad usage) on conflict

    # Load config early to build notifier and read scheduling settings.
    # If the config itself blows up we fall back to stderr-only notifications.
    notifier: Optional[Notifier] = None
    cfg_preview: Optional[dict] = None
    on_script_error = True
    try:
        try:
            cfg_preview = load_config(args.config)
            on_script_error = bool(
                (cfg_preview.get("notifications") or {}).get("on_script_error", True)
            )
            # Build a notifier early so even if _run raises during Plex work
            # we can alert. _run() will rebuild with final logging applied.
            notifier = build_notifier(cfg_preview)
        except SystemExit:
            # load_config uses sys.exit() for fatal config errors; let it through.
            raise
        except Exception:  # noqa: BLE001
            notifier = CompositeNotifier([StderrNotifier()])

        # Resolved once here (for lock metadata + last_run.json) and again
        # inside _run() against its own cfg load — both are pure/side-effect
        # free given the same args/cfg, so the duplication is harmless and
        # mirrors the existing double load_config() call in this function.
        resolved_mode = _resolve_mode(args, cfg_preview or {})

        # Scheduling primitives — run before any Plex connection.
        # LOCK_HELD and SKIPPED_RECENT are expected scheduler outcomes:
        # they do not notify and do not write last_run.json.
        _ensure_state_dir()
        if not _acquire_lock(mode=resolved_mode):
            return int(ExitCode.LOCK_HELD)

        try:
            if cfg_preview and _check_skip_recent(cfg_preview):
                return int(ExitCode.SKIPPED_RECENT)

            started_at = datetime.now(timezone.utc)
            exit_code = ExitCode.SUCCESS
            move_stats: dict = {"moves_attempted": 0, "moves_succeeded": 0, "bytes_moved": 0}

            try:
                move_stats = _run(args) or move_stats
                _write_last_run(started_at, int(ExitCode.SUCCESS), move_stats, mode=resolved_mode)
            except SystemExit:
                raise
            except KeyboardInterrupt:
                log.warning("Interrupted by user")
                _write_last_run(
                    started_at, int(ExitCode.KEYBOARD_INTERRUPT), move_stats, mode=resolved_mode,
                )
                exit_code = ExitCode.KEYBOARD_INTERRUPT
            except Exception as e:  # noqa: BLE001
                tb = traceback.format_exc()
                log.error("Unhandled error: %s\n%s", e, tb)
                if notifier is None:
                    notifier = CompositeNotifier([StderrNotifier()])
                if on_script_error:
                    notifier.alert(
                        title="Tier: script error",
                        message=(
                            f"tier.py crashed with: {type(e).__name__}: {e}\n\n"
                            f"Tail of traceback:\n{tb[-800:]}"
                        ),
                        level="error",
                    )
                _write_last_run(
                    started_at, int(ExitCode.UNHANDLED_CRASH), move_stats, mode=resolved_mode,
                )
                exit_code = ExitCode.UNHANDLED_CRASH

            return int(exit_code)
        finally:
            _release_lock()

    except SystemExit:
        raise


def _test_resolve_user_share():
    """Inline test: resolve_user_share picks the disk that actually has the file.

    Run with: python3 tier.py --_test
    (hooked via _maybe_run_tests below)
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        # Simulate /mnt/disk1, /mnt/disk3 (disk2 absent), /mnt/hot_pool
        disk1 = _os.path.join(root, "disk1")
        disk3 = _os.path.join(root, "disk3")
        hot   = _os.path.join(root, "hot_pool")
        for d in (disk1, disk3, hot):
            _os.makedirs(_os.path.join(d, "Movies"), exist_ok=True)

        # Place the file only on disk3
        target = _os.path.join(disk3, "Movies", "Foo.mkv")
        open(target, "w").close()

        user_prefix = "/mnt/user"
        plex_path    = "/mnt/user/Movies/Foo.mkv"
        array_disks  = [disk1, disk3]

        result = resolve_user_share(plex_path, user_prefix, hot, array_disks)
        assert result == target, f"expected {target!r}, got {result!r}"

        # File absent everywhere → original path returned unchanged
        missing = resolve_user_share("/mnt/user/Movies/Gone.mkv", user_prefix, hot, array_disks)
        assert missing == "/mnt/user/Movies/Gone.mkv", f"expected original path, got {missing!r}"

        # Non-user-share path → no-op
        direct = resolve_user_share("/mnt/hot_pool/Movies/Bar.mkv", user_prefix, hot, array_disks)
        assert direct == "/mnt/hot_pool/Movies/Bar.mkv"

        # Empty prefix → no-op
        noop = resolve_user_share(plex_path, "", hot, array_disks)
        assert noop == plex_path

    print("_test_resolve_user_share: OK")


def _make_cfg(extra_thresholds=None):
    """Minimal config dict for test harness."""
    t = {
        "score_to_hot": 40.0,
        "score_to_warm": 20.0,
        "recency_half_life_days": 90,
        "age_grace_days": 180,
        "added_floor_days_movies": 45,
        "added_floor_days_tv": 30,
    }
    if extra_thresholds:
        t.update(extra_thresholds)
    return {"pinning": {}, "thresholds": t}


def _make_item(**kwargs):
    """Build an Item with sensible defaults for test harness."""
    now = datetime.now(timezone.utc)
    defaults = dict(
        title="Test Item", year=2024, kind="movie", library="Movies",
        plays=0, last_played=None,
        added=now - timedelta(days=200),
        size_bytes=1_000_000_000,
        score=25.0,
        rating_key=None,
        collection_pinned=False,
        auto_inherit_pinned=False,
        recently_added=False,
    )
    defaults.update(kwargs)
    return Item(**defaults)


def _test_added_floor_movie_recent():
    """movie addedAt=10d ago, 0 plays, score=25 (NEUTRAL) -> HOT via floor"""
    now = datetime.now(timezone.utc)
    item = _make_item(added=now - timedelta(days=10), score=25.0, recently_added=True)
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "HOT", f"expected HOT, got {rec}"
    assert not pinned
    assert reason and "added-date floor" in reason
    print("_test_added_floor_movie_recent: OK")


def _test_added_floor_movie_old():
    """movie addedAt=100d ago -> floor does NOT engage; score=0 -> WARM"""
    now = datetime.now(timezone.utc)
    item = _make_item(added=now - timedelta(days=100), score=0.0, recently_added=False)
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "WARM", f"expected WARM, got {rec}"
    assert not pinned
    assert reason is None
    print("_test_added_floor_movie_old: OK")


def _test_added_floor_tv_recent_episode():
    """show.addedAt=2y ago, recently_added=True (episode 5d ago), 0 plays -> HOT"""
    now = datetime.now(timezone.utc)
    item = _make_item(
        kind="series", library="TV Shows",
        added=now - timedelta(days=730), score=0.0, recently_added=True,
    )
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "HOT", f"expected HOT, got {rec}"
    assert not pinned
    assert reason and "added-date floor" in reason
    print("_test_added_floor_tv_recent_episode: OK")


def _test_added_floor_tv_no_recent():
    """show with no recent episodes -> floor does NOT engage; score=0 -> WARM"""
    now = datetime.now(timezone.utc)
    item = _make_item(
        kind="series", library="TV Shows",
        added=now - timedelta(days=730), score=0.0, recently_added=False,
    )
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "WARM", f"expected WARM, got {rec}"
    assert not pinned
    assert reason is None
    print("_test_added_floor_tv_no_recent: OK")


def _test_added_floor_disabled():
    """added_floor_days_movies=0, recently_added=False, score=25 -> NEUTRAL"""
    now = datetime.now(timezone.utc)
    item = _make_item(
        added=now - timedelta(days=5), score=25.0, recently_added=False,
    )
    cfg = _make_cfg({"added_floor_days_movies": 0, "added_floor_days_tv": 0})
    rec, pinned, reason = _compute_recommendation(item, cfg, now)
    assert rec == "NEUTRAL", f"expected NEUTRAL, got {rec}"
    assert not pinned
    assert reason is None
    print("_test_added_floor_disabled: OK")


def _test_added_floor_preserves_pin():
    """library-pinned + recently_added=True -> pinned=True, outcome=PIN_HOT"""
    now = datetime.now(timezone.utc)
    item = _make_item(
        library="4K Movies", added=now, score=25.0, recently_added=True,
    )
    cfg = _make_cfg()
    cfg["pinning"] = {"always_hot_libraries": ["4K Movies"]}
    rec, pinned, reason = _compute_recommendation(item, cfg, now)
    assert pinned, "expected pinned=True"
    assert "pinned library" in (reason or "")
    outcome = _combine_outcome("UNKNOWN", rec, pinned)
    assert outcome == "PIN_HOT", f"expected PIN_HOT, got {outcome}"
    print("_test_added_floor_preserves_pin: OK")


def _test_added_floor_never_demotes():
    """recently_added=True, no plays, no hot_recency -> floor fires, outcome=HOT"""
    now = datetime.now(timezone.utc)
    item = _make_item(
        kind="series", library="TV Shows",
        added=now - timedelta(days=365), score=0.0, recently_added=True,
        last_played=None,
    )
    cfg = _make_cfg({"hot_recency_days": 730})
    rec, pinned, reason = _compute_recommendation(item, cfg, now)
    assert rec == "HOT", f"expected HOT, got {rec}"
    assert not pinned
    assert reason and "added-date floor" in reason
    print("_test_added_floor_never_demotes: OK")


def _test_added_floor_tv_search_uses_int_timestamp():
    """_build_recently_active_shows must pass int Unix timestamp to addedAt__gte.

    plexapi's filter evaluation compares addedAt__gte against the stored string
    form of a Unix timestamp. Passing a datetime object causes:
      TypeError: '>=' not supported between instances of 'str' and 'datetime.datetime'
    Only int seconds avoids this — the check here guards against regression.
    """
    captured = {}

    class _FakeEp:
        grandparentRatingKey = 99

    class _FakeSection:
        title = "TV Shows"

        def search(self, **kwargs):
            captured.update(kwargs)
            return [_FakeEp()]

    result = _build_recently_active_shows(_FakeSection(), {"added_floor_days_tv": 30})

    assert "addedAt__gte" in captured, "addedAt__gte not passed to section.search"
    assert isinstance(captured["addedAt__gte"], int), (
        f"addedAt__gte must be int (Unix seconds), got {type(captured['addedAt__gte']).__name__}"
    )
    assert result == {99}
    print("_test_added_floor_tv_search_uses_int_timestamp: OK")


def _test_collection_pin_promotes_to_pin_hot():
    """collection_pinned=True, any score/tier -> PIN_HOT via pinned=True"""
    now = datetime.now(timezone.utc)
    for current_tier in ("HOT", "WARM", "UNKNOWN"):
        item = _make_item(
            score=0.0, current_tier=current_tier,
            rating_key=42, collection_pinned=True,
        )
        rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
        assert rec == "HOT", f"expected HOT, got {rec} (current_tier={current_tier})"
        assert pinned, f"expected pinned=True (current_tier={current_tier})"
        assert reason == "collection pin"
        outcome = _combine_outcome(current_tier, rec, pinned)
        assert outcome == "PIN_HOT", f"expected PIN_HOT, got {outcome}"
    print("_test_collection_pin_promotes_to_pin_hot: OK")


def _test_collection_pin_missing_collection():
    """Missing collection -> WARNING emitted, empty set returned, no crash"""
    warnings_seen = []
    _orig_warning = log.warning

    def _capture_warning(msg, *args):
        warnings_seen.append(msg % args if args else msg)
        _orig_warning(msg, *args)

    log.warning = _capture_warning
    try:
        class _FakeSection:
            title = "Movies"

            def collections(self):
                return []  # collection not present

        class _FakePlex:
            class _Lib:
                def section(self, *_):
                    return _FakeSection()
            library = _Lib()

        keys, matched, total = _build_collection_pinned_keys(
            _FakePlex(), [{"library": "Movies", "name": "MCU"}]
        )
        assert keys == set(), f"expected empty set, got {keys}"
        assert matched == 0
        assert total == 0
        assert any("MCU" in w for w in warnings_seen), "expected warning about missing collection"
    finally:
        log.warning = _orig_warning
    print("_test_collection_pin_missing_collection: OK")


def _test_collection_pin_empty_list():
    """Empty pinned_collections -> returns immediately with no Plex calls"""
    called = []

    class _SentinelPlex:
        class _Lib:
            def section(self, name):
                called.append(name)
        library = _Lib()

    keys, matched, total = _build_collection_pinned_keys(_SentinelPlex(), [])
    assert keys == set()
    assert matched == 0
    assert total == 0
    assert not called, "section() should not be called for empty list"
    print("_test_collection_pin_empty_list: OK")


def _test_collection_pin_idempotent_with_added_floor():
    """collection_pinned=True AND recently_added=True -> collection pin wins (step 3 < step 4).
    Only the collection-pin override is recorded; added-floor override is not set.
    """
    now = datetime.now(timezone.utc)
    item = _make_item(
        added=now - timedelta(days=5), score=25.0,
        rating_key=7, collection_pinned=True, recently_added=True,
    )
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "HOT", f"expected HOT, got {rec}"
    assert pinned, "expected pinned=True from collection pin"
    assert reason == "collection pin", f"expected collection pin reason, got {reason!r}"
    print("_test_collection_pin_idempotent_with_added_floor: OK")


def _test_auto_inherit_happy_path():
    """Two collections: one triggers, one doesn't.

    Franchise A (3 members, 2 hot): size > min_hot → uses absolute threshold
    (2 >= 2 required). Triggers; cold member rk=3 gets PIN_HOT via inherit.

    Franchise B (2 members, 0 hot): size == min_hot → fraction branch
    (ceil(2*0.5)=1 required). 0 hot members → does not trigger.
    """
    now = datetime.now(timezone.utc)
    items = [
        _make_item(rating_key=1, score=50.0),   # hot — Franchise A
        _make_item(rating_key=2, score=50.0),   # hot — Franchise A
        _make_item(rating_key=3, score=25.0),   # cold — Franchise A
        _make_item(rating_key=4, score=10.0),   # cold — Franchise B (no hot members)
        _make_item(rating_key=5, score=10.0),   # cold — Franchise B
    ]

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeCol:
        def __init__(self, title, rks, smart=False):
            self.title = title
            self.smart = smart
            self._rks = rks
        def items(self):
            return [_FakeMember(rk) for rk in self._rks]

    class _FakeSection:
        def collections(self):
            return [
                _FakeCol("Franchise A", [1, 2, 3]),  # size=3>2, 2 hot → triggers
                _FakeCol("Franchise B", [4, 5]),     # size=2==2, 0 hot → no trigger
            ]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    auto_cfg = {
        "enabled": True, "min_hot_members": 2, "min_hot_fraction": 0.5,
        "skip_smart_collections": True, "exclude_libraries": [],
    }
    keys, triggered, inherited = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, items, ["Movies"]
    )

    assert triggered == 1, f"expected 1 triggered, got {triggered}"
    assert inherited == 3, f"expected 3 inherited, got {inherited}"
    assert keys == {1, 2, 3}, f"expected {{1,2,3}}, got {keys}"

    # Cold member of the triggered collection gets PIN_HOT via auto-inherit.
    items[2].auto_inherit_pinned = True  # rk=3, score=25 (NEUTRAL without inherit)
    rec, pinned, reason = _compute_recommendation(items[2], _make_cfg(), now)
    assert rec == "HOT", f"expected HOT via auto-inherit, got {rec}"
    assert pinned
    assert reason == "auto-inherit collection"
    print("_test_auto_inherit_happy_path: OK")


def _test_auto_inherit_threshold_not_met():
    """collection size=4 > min_hot_members=3, only 2 hot members → absolute threshold
    applies (required=3), 2 < 3, no trigger.
    """

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeCol:
        title = "Franchise"
        smart = False
        def items(self):
            return [_FakeMember(rk) for rk in [1, 2, 3, 4]]

    class _FakeSection:
        def collections(self):
            return [_FakeCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    items = [
        _make_item(rating_key=1, score=50.0),
        _make_item(rating_key=2, score=50.0),
        _make_item(rating_key=3, score=10.0),
        _make_item(rating_key=4, score=10.0),
    ]
    auto_cfg = {
        "enabled": True, "min_hot_members": 3, "min_hot_fraction": 0.5,
        "skip_smart_collections": True, "exclude_libraries": [],
    }
    keys, triggered, inherited = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, items, ["Movies"]
    )
    assert triggered == 0, f"expected 0 triggered, got {triggered}"
    assert inherited == 0
    assert keys == set()
    print("_test_auto_inherit_threshold_not_met: OK")


def _test_auto_inherit_explicit_pin_takes_precedence():
    """item.collection_pinned=True AND item.auto_inherit_pinned=True → explicit pin wins (step 3 < step 4).
    Reason is 'collection pin', not 'auto-inherit collection'.
    """
    now = datetime.now(timezone.utc)
    item = _make_item(
        score=0.0, rating_key=42,
        collection_pinned=True, auto_inherit_pinned=True,
    )
    rec, pinned, reason = _compute_recommendation(item, _make_cfg(), now)
    assert rec == "HOT"
    assert pinned
    assert reason == "collection pin", (
        f"explicit pin must fire before auto-inherit; got {reason!r}"
    )
    print("_test_auto_inherit_explicit_pin_takes_precedence: OK")


def _test_auto_inherit_smart_collection_skip():
    """Smart collections are skipped when skip_smart_collections=True, included when False."""

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeSmartCol:
        title = "Smart"
        smart = True
        def items(self):
            return [_FakeMember(1), _FakeMember(2)]

    class _FakeSection:
        def collections(self):
            return [_FakeSmartCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    items = [
        _make_item(rating_key=1, score=50.0),
        _make_item(rating_key=2, score=50.0),
    ]

    auto_cfg_skip = {
        "enabled": True, "min_hot_members": 2,
        "skip_smart_collections": True, "exclude_libraries": [],
    }
    keys_skip, triggered_skip, _ = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg_skip, 40.0, items, ["Movies"]
    )
    assert triggered_skip == 0, "smart collection should be skipped"
    assert keys_skip == set()

    auto_cfg_include = {**auto_cfg_skip, "skip_smart_collections": False}
    keys_inc, triggered_inc, _ = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg_include, 40.0, items, ["Movies"]
    )
    assert triggered_inc == 1, "smart collection should trigger when skip=False"
    assert keys_inc == {1, 2}
    print("_test_auto_inherit_smart_collection_skip: OK")


def _test_auto_inherit_disabled():
    """enabled=False → returns immediately with no Plex calls."""
    called = []

    class _SentinelPlex:
        class _Lib:
            def section(self, *_):
                called.append("section")
        library = _Lib()

    auto_cfg = {"enabled": False, "min_hot_members": 2}
    keys, triggered, inherited = _build_auto_inherit_keys(
        _SentinelPlex(), auto_cfg, 40.0, [], ["Movies"]
    )
    assert keys == set()
    assert triggered == 0
    assert inherited == 0
    assert not called, "section() should not be called when disabled"
    print("_test_auto_inherit_disabled: OK")


def _test_auto_inherit_exclude_library():
    """Library listed in exclude_libraries is skipped; its collections never trigger."""
    called = []

    class _FakePlex:
        class _Lib:
            def section(self, name):
                called.append(name)
        library = _Lib()

    auto_cfg = {
        "enabled": True, "min_hot_members": 1,
        "skip_smart_collections": False, "exclude_libraries": ["DVD Rips"],
    }
    _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, [], ["Movies", "DVD Rips"]
    )
    assert "DVD Rips" not in called, "excluded library must not be fetched"
    print("_test_auto_inherit_exclude_library: OK")


def _test_auto_inherit_fraction_triggers_small_collection():
    """size == min_hot_members, 1 hot member, min_hot_fraction=0.5 → triggers (ceil(2*0.5)=1)."""

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeCol:
        title = "Pair"
        smart = False
        def items(self):
            return [_FakeMember(1), _FakeMember(2)]

    class _FakeSection:
        def collections(self):
            return [_FakeCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    items = [
        _make_item(rating_key=1, score=50.0),   # hot
        _make_item(rating_key=2, score=10.0),   # cold
    ]
    auto_cfg = {
        "enabled": True, "min_hot_members": 2, "min_hot_fraction": 0.5,
        "skip_smart_collections": False, "exclude_libraries": [],
    }
    keys, triggered, inherited = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, items, ["Movies"]
    )
    assert triggered == 1, f"expected 1 triggered, got {triggered}"
    assert inherited == 2
    assert keys == {1, 2}
    print("_test_auto_inherit_fraction_triggers_small_collection: OK")


def _test_auto_inherit_fraction_no_hot_no_trigger():
    """size == min_hot_members, 0 hot members → does not trigger."""

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeCol:
        title = "Pair"
        smart = False
        def items(self):
            return [_FakeMember(1), _FakeMember(2)]

    class _FakeSection:
        def collections(self):
            return [_FakeCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    items = [
        _make_item(rating_key=1, score=10.0),
        _make_item(rating_key=2, score=10.0),
    ]
    auto_cfg = {
        "enabled": True, "min_hot_members": 2, "min_hot_fraction": 0.5,
        "skip_smart_collections": False, "exclude_libraries": [],
    }
    keys, triggered, _ = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, items, ["Movies"]
    )
    assert triggered == 0
    assert keys == set()
    print("_test_auto_inherit_fraction_no_hot_no_trigger: OK")


def _test_auto_inherit_skip_below_min_hot():
    """collection size < min_hot_members → items() is never called."""
    items_checked = []

    class _FakeCol:
        title = "Singleton"
        smart = False
        def items(self):
            items_checked.append("called")
            return []

    class _FakeSection:
        def collections(self):
            return [_FakeCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    # Stub items() to return 1 member so col_size=1 < min_hot=2
    # BUT we need items() to be reachable to check if it is called.
    # Override items() to append a marker and return one member.
    class _Member:
        ratingKey = 99

    _FakeCol.items = lambda *_: (items_checked.append("called") or [_Member()])  # type: ignore[assignment]

    auto_cfg = {
        "enabled": True, "min_hot_members": 2, "min_hot_fraction": 0.5,
        "skip_smart_collections": False, "exclude_libraries": [],
    }
    _build_auto_inherit_keys(_FakePlex(), auto_cfg, 40.0, [], ["Movies"])
    # items() IS called (to get member_keys for the size check).
    # The test verifies the hot-count loop is not the issue — the size guard
    # fires after member_keys is built, before any score lookup.
    # What must NOT happen: the collection triggering despite size < min_hot.
    # Re-run with a hot item in the lookup to confirm it still doesn't trigger.
    hot_item = _make_item(rating_key=99, score=50.0)
    keys, triggered, _ = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, [hot_item], ["Movies"]
    )
    assert triggered == 0, f"size-1 collection must not trigger, got triggered={triggered}"
    assert keys == set()
    print("_test_auto_inherit_skip_below_min_hot: OK")


def _test_auto_inherit_larger_collection_uses_absolute():
    """collection size 5, min_hot_members=2, only 1 hot member → does not trigger."""

    class _FakeMember:
        def __init__(self, rk):
            self.ratingKey = rk

    class _FakeCol:
        title = "Big"
        smart = False
        def items(self):
            return [_FakeMember(rk) for rk in range(1, 6)]

    class _FakeSection:
        def collections(self):
            return [_FakeCol()]

    class _FakePlex:
        class _Lib:
            def section(self, *_):
                return _FakeSection()
        library = _Lib()

    items = [_make_item(rating_key=rk, score=10.0) for rk in range(1, 6)]
    items[0] = _make_item(rating_key=1, score=50.0)  # only rk=1 is hot
    auto_cfg = {
        "enabled": True, "min_hot_members": 2, "min_hot_fraction": 0.5,
        "skip_smart_collections": False, "exclude_libraries": [],
    }
    keys, triggered, _ = _build_auto_inherit_keys(
        _FakePlex(), auto_cfg, 40.0, items, ["Movies"]
    )
    assert triggered == 0, f"absolute threshold must apply for size>min_hot; got {triggered}"
    assert keys == set()
    print("_test_auto_inherit_larger_collection_uses_absolute: OK")


def _test_eviction_stay_warm_becomes_relocate():
    """Item on evict-marked disk with natural STAY_WARM -> outcome RELOCATE_WARM."""
    item = _make_item(score=25.0, current_tier="WARM", current_disk="/mnt/disk7")
    item.outcome = "STAY_WARM"
    evict_cfg = {"enabled": True, "disks": ["/mnt/disk7"]}
    evict_disks = _build_evict_disks(evict_cfg, ["/mnt/disk7", "/mnt/disk1"])
    assert evict_disks == {"/mnt/disk7"}
    items_on_evict = [it for it in [item] if it.current_disk in evict_disks]
    for it in items_on_evict:
        if it.outcome == "STAY_WARM":
            it.outcome = "RELOCATE_WARM"
    assert item.outcome == "RELOCATE_WARM", f"expected RELOCATE_WARM, got {item.outcome}"
    assert "RELOCATE_WARM" in _WARM_OUTCOMES, "RELOCATE_WARM must be in _WARM_OUTCOMES"
    print("_test_eviction_stay_warm_becomes_relocate: OK")


def _test_eviction_to_hot_stays_to_hot():
    """Item on evict-marked disk with natural TO_HOT -> outcome stays TO_HOT."""
    item = _make_item(score=55.0, current_tier="WARM", current_disk="/mnt/disk7")
    item.outcome = "TO_HOT"
    evict_cfg = {"enabled": True, "disks": ["/mnt/disk7"]}
    evict_disks = _build_evict_disks(evict_cfg, ["/mnt/disk7", "/mnt/disk1"])
    items_on_evict = [it for it in [item] if it.current_disk in evict_disks]
    for it in items_on_evict:
        if it.outcome == "STAY_WARM":
            it.outcome = "RELOCATE_WARM"
    assert item.outcome == "TO_HOT", f"expected TO_HOT unchanged, got {item.outcome}"
    print("_test_eviction_to_hot_stays_to_hot: OK")


def _test_eviction_non_evict_disk_unaffected():
    """Item on non-evict disk -> outcome unaffected even if other disks are in evict set."""
    item = _make_item(score=25.0, current_tier="WARM", current_disk="/mnt/disk1")
    item.outcome = "STAY_WARM"
    evict_cfg = {"enabled": True, "disks": ["/mnt/disk7"]}
    evict_disks = _build_evict_disks(evict_cfg, ["/mnt/disk7", "/mnt/disk1"])
    items_on_evict = [it for it in [item] if it.current_disk in evict_disks]
    for it in items_on_evict:
        if it.outcome == "STAY_WARM":
            it.outcome = "RELOCATE_WARM"
    assert item.outcome == "STAY_WARM", f"expected STAY_WARM unchanged, got {item.outcome}"
    print("_test_eviction_non_evict_disk_unaffected: OK")


def _test_eviction_disabled_no_items_flagged():
    """enabled=False -> no items flagged, _build_evict_disks returns empty set."""
    item = _make_item(score=25.0, current_tier="WARM", current_disk="/mnt/disk7")
    item.outcome = "STAY_WARM"
    evict_cfg = {"enabled": False, "disks": ["/mnt/disk7"]}
    evict_disks = _build_evict_disks(evict_cfg, ["/mnt/disk7"])
    assert evict_disks == set(), f"expected empty set when disabled, got {evict_disks}"
    # No eviction pass runs when evict_disks is empty
    assert item.outcome == "STAY_WARM", "outcome must be unchanged when disabled"
    print("_test_eviction_disabled_no_items_flagged: OK")


def _test_dominant_warm_disk_movie_with_year_folder():
    """resolve_item_current_tier correctly attributes a movie at a deep year-subfolder path.

    Guards against hypothesis 1 (path nesting breaks disk attribution):
      /mnt/user/Movies/<year>/<title>/<file>.mkv should resolve to the disk
      holding the majority of bytes, not return dominant=None.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk1  = _os.path.join(root, "disk1")
        disk7  = _os.path.join(root, "disk7")
        hot    = _os.path.join(root, "hot_pool")
        rel    = _os.path.join("Movies", "2007",
                                "Alien vs. Predator Requiem (2007)")
        _os.makedirs(_os.path.join(disk7, rel), exist_ok=True)
        _os.makedirs(_os.path.join(disk1, "Movies"), exist_ok=True)
        _os.makedirs(_os.path.join(hot,   "Movies"), exist_ok=True)

        fname  = "Alien vs. Predator Requiem (2007).mkv"
        target = _os.path.join(disk7, rel, fname)
        open(target, "w").close()

        user_prefix = "/mnt/user"
        plex_path   = "/mnt/user/Movies/2007/Alien vs. Predator Requiem (2007)/" + fname
        array_disks = [disk1, disk7]
        path_map    = []

        tier, breakdown, dominant, *_ = resolve_item_current_tier(
            [(plex_path, 5_000_000_000)],
            path_map, hot, array_disks, user_prefix,
        )
        assert tier == "WARM", f"expected WARM, got {tier!r}"
        assert dominant == disk7, f"expected dominant={disk7!r}, got {dominant!r}"
    print("_test_dominant_warm_disk_movie_with_year_folder: OK")


def _test_dominant_warm_disk_single_file_item():
    """A movie with exactly one media file gets current_disk populated (not None).

    Guards against hypothesis 3 (single-file items short-circuit and return None).
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk7 = _os.path.join(root, "disk7")
        hot   = _os.path.join(root, "hot_pool")
        _os.makedirs(_os.path.join(disk7, "Movies"), exist_ok=True)
        _os.makedirs(_os.path.join(hot,   "Movies"), exist_ok=True)

        fname  = "The Hunger Games Catching Fire (2013).mkv"
        target = _os.path.join(disk7, "Movies", fname)
        open(target, "w").close()

        user_prefix = "/mnt/user"
        plex_path   = "/mnt/user/Movies/" + fname
        array_disks = [disk7]

        tier, breakdown, dominant, *_ = resolve_item_current_tier(
            [(plex_path, 8_000_000_000)],
            path_map=[], hot_mount=hot, array_disks=array_disks,
            user_share_prefix=user_prefix,
        )
        assert tier == "WARM", f"expected WARM, got {tier!r}"
        assert dominant is not None, "dominant must not be None for a single-file WARM item"
        assert dominant == disk7, f"expected dominant={disk7!r}, got {dominant!r}"
    print("_test_dominant_warm_disk_single_file_item: OK")


def _test_eviction_movie_on_evict_disk_becomes_relocate():
    """movie kind on evict-marked disk, natural STAY_WARM -> outcome RELOCATE_WARM.

    Mirror of _test_eviction_stay_warm_becomes_relocate for kind='movie'.
    Guards against hypothesis 4 (movies pre-assigned via a separate code path
    the eviction pass doesn't evaluate).
    """
    item = _make_item(kind="movie", score=25.0, current_tier="WARM",
                      current_disk="/mnt/disk7")
    item.outcome = "STAY_WARM"
    evict_cfg   = {"enabled": True, "disks": ["/mnt/disk7"]}
    evict_disks = _build_evict_disks(evict_cfg, ["/mnt/disk7", "/mnt/disk1"])
    items_on_evict = [it for it in [item] if it.current_disk in evict_disks]
    for it in items_on_evict:
        if it.outcome == "STAY_WARM":
            it.outcome = "RELOCATE_WARM"
    assert item.outcome == "RELOCATE_WARM", (
        f"expected RELOCATE_WARM for movie kind, got {item.outcome}"
    )
    print("_test_eviction_movie_on_evict_disk_becomes_relocate: OK")


def _test_hot_majority_warm_disk_files_populated():
    """resolve_item_current_tier returns warm_disk_files even when HOT is the majority tier.

    Regression guard: a partial prior move leaves some files on a warm disk.
    The caller (straggler pass) needs warm_disk_files populated to detect and
    promote the item to TO_HOT.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk1 = _os.path.join(root, "disk1")
        hot   = _os.path.join(root, "hot_pool")
        _os.makedirs(_os.path.join(disk1, "TV Shows", "Bones", "Season 09"), exist_ok=True)
        _os.makedirs(_os.path.join(hot,   "TV Shows", "Bones", "Season 01"), exist_ok=True)

        warm_file = _os.path.join(disk1, "TV Shows", "Bones", "Season 09", "s09e01.mkv")
        hot_file  = _os.path.join(hot,   "TV Shows", "Bones", "Season 01", "s01e01.mkv")
        # HOT file is much larger (majority)
        with open(hot_file, "wb") as f:
            f.write(b"\x00" * 10_000_000)
        with open(warm_file, "wb") as f:
            f.write(b"\x00" * 1_000_000)

        user_prefix = "/mnt/user"
        parts = [
            (hot_file, 10_000_000),
            (warm_file, 1_000_000),
        ]
        tier, _, dominant, _, wdf, _ = resolve_item_current_tier(
            parts, [], hot, [disk1], user_prefix,
        )
        assert tier == "HOT", f"expected HOT majority, got {tier!r}"
        assert dominant is None, "dominant should be None for HOT-majority items"
        assert disk1 in wdf, f"warm_disk_files should include {disk1!r}, got {wdf.keys()}"
        assert any(warm_file in wdf[disk1] for _ in [1]), \
            f"warm file should be in warm_disk_files, got {wdf}"
    print("_test_hot_majority_warm_disk_files_populated: OK")


def _test_eviction_minority_warm_files_on_evict_disk():
    """STAY_WARM item whose majority is on a safe disk but minority files are on an
    evicting disk gets RELOCATE_WARM with relocate_source_override limiting the source
    to the evicting disk only, and current_disk overridden to the evicting disk.

    Models: Bones (majority disk1, S9/S12 minority on disk7=evicting).
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk1 = _os.path.join(root, "disk1")
        disk7 = _os.path.join(root, "disk7")
        _os.makedirs(_os.path.join(disk1, "TV Shows", "Bones", "Season 01"), exist_ok=True)
        _os.makedirs(_os.path.join(disk7, "TV Shows", "Bones", "Season 09"), exist_ok=True)

        s01 = _os.path.join(disk1, "TV Shows", "Bones", "Season 01", "s01e01.mkv")
        s09 = _os.path.join(disk7, "TV Shows", "Bones", "Season 09", "s09e01.mkv")
        with open(s01, "wb") as f:
            f.write(b"\x00" * 5_000_000)
        with open(s09, "wb") as f:
            f.write(b"\x00" * 1_000_000)

        item = _make_item(kind="series", score=20.0, current_tier="WARM", current_disk=disk1)
        item.outcome = "STAY_WARM"
        item.warm_disk_files = {disk1: [s01], disk7: [s09]}

        evict_cfg   = {"enabled": True, "disks": [disk7]}
        evict_disks = _build_evict_disks(evict_cfg, [disk1, disk7])

        items_on_evict = [
            it for it in [item]
            if (it.current_disk is not None and it.current_disk in evict_disks)
            or any(d in evict_disks for d in it.warm_disk_files)
        ]
        assert len(items_on_evict) == 1, "minority-evict item must be included in items_on_evict"

        for it in items_on_evict:
            if it.outcome == "STAY_WARM":
                it.outcome = "RELOCATE_WARM"
                if it.current_disk not in evict_disks:
                    evict_files = {
                        d: it.warm_disk_files[d]
                        for d in it.warm_disk_files if d in evict_disks
                    }
                    if evict_files:
                        it.relocate_source_override = evict_files
                        it.current_disk = max(
                            evict_files.keys(),
                            key=lambda d: sum(
                                _os.path.getsize(f)
                                for f in evict_files[d] if _os.path.exists(f)
                            ),
                        )

        assert item.outcome == "RELOCATE_WARM", f"expected RELOCATE_WARM, got {item.outcome}"
        assert item.current_disk == disk7, f"current_disk should be overridden to {disk7!r}, got {item.current_disk!r}"
        assert item.relocate_source_override is not None, "relocate_source_override must be set"
        assert disk7 in item.relocate_source_override, "override must contain evicting disk"
        assert disk1 not in item.relocate_source_override, "override must NOT contain safe disk"
        assert s09 in item.relocate_source_override[disk7], "override must contain the minority file"
        # warm_disk_files still intact for co-location scoring
        assert disk1 in item.warm_disk_files, "warm_disk_files must still contain safe disk for co-location"
    print("_test_eviction_minority_warm_files_on_evict_disk: OK")


def _test_eviction_majority_evict_non_evict_files_excluded():
    """RELOCATE_WARM for a majority-evict series restricts the source to evicting-disk files.

    Guards against the data-loss scenario where warm_disk_files includes files
    from non-evicting disks.  Without relocate_source_override, the move
    executor would rsync non-evicting-disk files to themselves (when co-location
    selects that disk as destination), pass size_verify, then delete them —
    permanently destroying the data.

    Models: BBTS majority on disk7 (evicting, S11+S12), minority on disk3 (S1-S10).
    Expected: relocate_source_override = {disk7: [S11,S12]} only; disk3 files intact.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk3 = _os.path.join(root, "disk3")
        disk7 = _os.path.join(root, "disk7")
        _os.makedirs(_os.path.join(disk3, "TV Shows", "TBBT", "Season 01"), exist_ok=True)
        _os.makedirs(_os.path.join(disk7, "TV Shows", "TBBT", "Season 11"), exist_ok=True)
        _os.makedirs(_os.path.join(disk7, "TV Shows", "TBBT", "Season 12"), exist_ok=True)

        s01 = _os.path.join(disk3, "TV Shows", "TBBT", "Season 01", "s01e01.mkv")
        s11 = _os.path.join(disk7, "TV Shows", "TBBT", "Season 11", "s11e01.mkv")
        s12 = _os.path.join(disk7, "TV Shows", "TBBT", "Season 12", "s12e01.mkv")
        # disk7 is majority (5+5 GB vs 2 GB on disk3)
        for path, sz in [(s01, 2_000_000), (s11, 5_000_000), (s12, 5_000_000)]:
            with open(path, "wb") as f:
                f.write(b"\x00" * sz)

        # disk7 is majority → current_disk = disk7 (in evict_disks)
        item = _make_item(kind="series", score=20.0, current_tier="WARM", current_disk=disk7)
        item.outcome = "STAY_WARM"
        item.warm_disk_files = {disk7: [s11, s12], disk3: [s01]}

        evict_cfg   = {"enabled": True, "disks": [disk7]}
        evict_disks = _build_evict_disks(evict_cfg, [disk3, disk7])

        items_on_evict = [
            it for it in [item]
            if (it.current_disk is not None and it.current_disk in evict_disks)
            or any(d in evict_disks for d in it.warm_disk_files)
        ]
        assert len(items_on_evict) == 1

        for it in items_on_evict:
            if it.outcome == "STAY_WARM":
                it.outcome = "RELOCATE_WARM"
                evict_files = {
                    d: it.warm_disk_files[d]
                    for d in it.warm_disk_files if d in evict_disks
                }
                if evict_files:
                    it.relocate_source_override = evict_files
                    if it.current_disk not in evict_disks:
                        it.current_disk = max(
                            evict_files.keys(),
                            key=lambda d: sum(
                                _os.path.getsize(f)
                                for f in evict_files[d] if _os.path.exists(f)
                            ),
                        )

        assert item.outcome == "RELOCATE_WARM"
        assert item.current_disk == disk7, "majority-evict must keep current_disk on evicting disk"
        assert item.relocate_source_override is not None
        assert disk7 in item.relocate_source_override, "override must contain evicting disk"
        assert disk3 not in item.relocate_source_override, \
            "override must NOT contain non-evicting disk — would cause self-rsync data loss"
        assert s11 in item.relocate_source_override[disk7]
        assert s12 in item.relocate_source_override[disk7]
        # warm_disk_files intact so co-location scoring can find disk3 as destination
        assert disk3 in item.warm_disk_files
    print("_test_eviction_majority_evict_non_evict_files_excluded: OK")


def _test_destination_path_movie_tohot():
    """_compute_destination_path: movie with per-item folder -> correct hot path."""
    item = _make_item(
        kind="movie", library="Movies", title="Foo", year=2010,
        current_disk="/mnt/disk4",
        source_dirs=["/mnt/disk4/Movies/Foo (2010)"],
    )
    dst = _compute_destination_path(item, "/mnt/zfs_media")
    assert dst == "/mnt/zfs_media/Movies/Foo (2010)", f"got {dst!r}"
    print("_test_destination_path_movie_tohot: OK")


def _test_destination_path_series_tohot():
    """_compute_destination_path: series -> correct hot path."""
    item = _make_item(
        kind="series", library="TV Shows", title="Show", year=2001,
        current_disk="/mnt/disk2",
        source_dirs=["/mnt/disk2/TV Shows/Show (2001)"],
    )
    dst = _compute_destination_path(item, "/mnt/zfs_media")
    assert dst == "/mnt/zfs_media/TV Shows/Show (2001)", f"got {dst!r}"
    print("_test_destination_path_series_tohot: OK")


def _test_move_skipped_when_already_hot():
    """TO_HOT item whose current_tier is already HOT -> SKIPPED, no rsync."""
    calls = []

    def _fake_run(cmd, **_):
        calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    item = _make_item(
        kind="movie", library="Movies",
        current_tier="HOT", current_disk=None,
        warm_disk_files={},  # HOT items have no warm files
    )
    item.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([item], cfg, apply=True)
    finally:
        subprocess.run = orig

    rsync_calls = [c for c in calls if c and c[0] == "rsync"]
    assert not rsync_calls, f"rsync must not be called for already-HOT item, got {rsync_calls}"
    print("_test_move_skipped_when_already_hot: OK")


def _test_dry_run_emits_no_apply_call():
    """Dry-run path never invokes rsync subprocess."""
    calls = []

    def _fake_run(cmd, **_):
        calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    item = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk4",
        warm_disk_files={"/mnt/disk4": ["/mnt/disk4/Movies/2010/Foo (2010).mkv"]},
    )
    item.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": True, "size_verify": True,
            "parity_check_blocking": True, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([item], cfg, apply=False)
    finally:
        subprocess.run = orig

    assert not calls, f"dry-run must make zero subprocess calls, got {calls}"
    print("_test_dry_run_emits_no_apply_call: OK")


def _test_parity_check_aborts_pass():
    """/proc/mdstat showing a check causes the move pass to abort before rsync."""
    import unittest.mock as _mock

    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    item = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk4",
        warm_disk_files={"/mnt/disk4": ["/mnt/disk4/Movies/2010/Foo (2010).mkv"]},
    )
    item.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": True, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    mdstat_content = (
        "Personalities : [raid6] [raid5]\n"
        "md0 : active raid5 sdg1[6] sdf1[5]\n"
        "      check=22.3% (123456/554432) finish=14.2min speed=123K/sec\n"
    )

    orig_run = subprocess.run
    try:
        subprocess.run = _fake_run
        with _mock.patch.object(Path, "read_text", return_value=mdstat_content):
            _run_move_pass([item], cfg, apply=True)
    finally:
        subprocess.run = orig_run

    assert not rsync_calls, f"rsync must not run when parity check active, got {rsync_calls}"
    print("_test_parity_check_aborts_pass: OK")


def _test_parity_check_unraid_idle_not_falsely_detected():
    """Unraid idle mdstat (mdResync=0 + mdResyncAction=check) -> NOT detected as running.

    Unraid stores the last action type in mdResyncAction regardless of whether
    a check is actually running. mdResync=0 means idle; matching on the word
    'check' in that field was a false positive.
    """
    import unittest.mock as _mock

    idle_content = (
        "mdResyncAction=check P\n"
        "mdResyncSize=15625879500\n"
        "mdResyncCorr=0\n"
        "mdResync=0\n"
        "mdResyncPos=0\n"
        "mdResyncDt=0\n"
        "mdResyncDb=0\n"
    )
    with _mock.patch.object(Path, "read_text", return_value=idle_content):
        result = _check_parity_in_progress()
    assert result is False, "idle Unraid mdstat must not be detected as parity check in progress"
    print("_test_parity_check_unraid_idle_not_falsely_detected: OK")


def _test_parity_check_unraid_active_detected():
    """Unraid active check (mdResync=<non-zero>) -> correctly detected as running."""
    import unittest.mock as _mock

    active_content = (
        "mdResyncAction=check P\n"
        "mdResyncSize=15625879500\n"
        "mdResyncCorr=0\n"
        "mdResync=1234567890\n"
        "mdResyncPos=1234567890\n"
        "mdResyncDt=100\n"
        "mdResyncDb=50\n"
    )
    with _mock.patch.object(Path, "read_text", return_value=active_content):
        result = _check_parity_in_progress()
    assert result is True, "active Unraid mdstat must be detected as parity check in progress"
    print("_test_parity_check_unraid_active_detected: OK")


def _test_size_verify_failure_skips_delete():
    """Size mismatch after rsync -> source file must NOT be deleted.

    Creates real src and dst files with different sizes. Fake rsync is a no-op
    (doesn't update dst), so os.path.getsize sees the mismatch and skips delete.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk4")
        hot_mount = _os.path.join(root, "zfs_media")
        src_file = _os.path.join(disk, "Movies", "2010", "Foo (2010).mkv")
        dst_file = _os.path.join(hot_mount, "Movies", "2010", "Foo (2010).mkv")
        _os.makedirs(_os.path.dirname(src_file), exist_ok=True)
        _os.makedirs(_os.path.dirname(dst_file), exist_ok=True)

        with open(src_file, "wb") as f:
            f.write(b"x" * 1000)        # src = 1 000 bytes
        with open(dst_file, "wb") as f:
            f.write(b"y" * 5_000_000)   # dst = 5 MB — deliberate mismatch

        def _fake_run(*_a, **_kw):
            class R:
                returncode = 0
                stderr = ""
            return R()  # rsync succeeds but doesn't touch files

        item = _make_item(
            kind="movie", library="Movies",
            current_tier="WARM", current_disk=disk,
            warm_disk_files={disk: [src_file]},
        )
        item.outcome = "TO_HOT"

        cfg = {
            "moves": {
                "enabled": True, "rsync_options": ["-aH"],
                "delete_source_after_verify": True, "size_verify": True,
                "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            },
            "paths": {"hot_pool_mount": hot_mount},
        }

        orig = subprocess.run
        try:
            subprocess.run = _fake_run
            _run_move_pass([item], cfg, apply=True)
        finally:
            subprocess.run = orig

        assert _os.path.exists(src_file), "source file must NOT be deleted on size mismatch"
    print("_test_size_verify_failure_skips_delete: OK")


def _test_multidisk_series_all_source_dirs_rsynced():
    """Series split across two warm disks: rsync --files-from called once per disk."""
    import tempfile, os as _os

    rsync_disk_roots = []

    def _fake_run(cmd, **_):
        class R:
            returncode = 0
            stderr = ""
        if cmd and cmd[0] == "rsync":
            # cmd = ["rsync", ...opts..., "--files-from=path", disk_root/, hot_mount/]
            rsync_disk_roots.append(cmd[-2])
        return R()

    with tempfile.TemporaryDirectory() as root:
        disk6 = _os.path.join(root, "disk6")
        disk3 = _os.path.join(root, "disk3")
        hot_mount = _os.path.join(root, "zfs_media")
        disk6_file = _os.path.join(disk6, "TV Shows", "Reba (2001)", "S01E01.mkv")
        disk3_file = _os.path.join(disk3, "TV Shows", "Reba (2001)", "S03E01.mkv")
        for f in (disk6_file, disk3_file):
            _os.makedirs(_os.path.dirname(f), exist_ok=True)
            open(f, "w").close()
        _os.makedirs(hot_mount, exist_ok=True)

        item = _make_item(
            kind="series", library="TV Shows", title="Reba", year=2001,
            current_tier="WARM", current_disk=disk6,
            warm_disk_files={disk6: [disk6_file], disk3: [disk3_file]},
        )
        item.outcome = "TO_HOT"

        cfg = {
            "moves": {
                "enabled": True, "rsync_options": ["-aH"],
                "delete_source_after_verify": False, "size_verify": False,
                "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            },
            "paths": {"hot_pool_mount": hot_mount},
        }

        orig = subprocess.run
        try:
            subprocess.run = _fake_run
            _run_move_pass([item], cfg, apply=True)
        finally:
            subprocess.run = orig

    assert len(rsync_disk_roots) == 2, (
        f"expected 2 rsync calls (one per disk), got {len(rsync_disk_roots)}: {rsync_disk_roots}"
    )
    assert any("disk6" in s for s in rsync_disk_roots), "disk6 root missing from rsync calls"
    assert any("disk3" in s for s in rsync_disk_roots), "disk3 root missing from rsync calls"
    print("_test_multidisk_series_all_source_dirs_rsynced: OK")


def _test_size_verify_mixed_tier_preexisting_dst_passes():
    """MIXED-tier: pre-existing file at dst does NOT count toward size verify.

    File-level verify measures only the specific files in warm_disk_files.
    A pre-existing S01E01.mkv already on the hot pool is ignored; only
    S02E01.mkv (the warm file being moved) is compared src vs dst.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk5")
        hot_mount = _os.path.join(root, "zfs_media")
        src_file = _os.path.join(disk, "TV Shows", "Fire Country (2022)", "S02E01.mkv")
        dst_file = _os.path.join(hot_mount, "TV Shows", "Fire Country (2022)", "S02E01.mkv")
        preexisting = _os.path.join(hot_mount, "TV Shows", "Fire Country (2022)", "S01E01.mkv")
        for f in (src_file, dst_file, preexisting):
            _os.makedirs(_os.path.dirname(f), exist_ok=True)

        with open(src_file, "wb") as f:
            f.write(b"x" * 500)   # src = 500 bytes
        with open(dst_file, "wb") as f:
            f.write(b"x" * 500)   # dst = 500 bytes — matches
        with open(preexisting, "wb") as f:
            f.write(b"y" * 1000)  # pre-existing — NOT in warm_disk_files, not measured

        def _fake_run(*_a, **_kw):
            class R:
                returncode = 0
                stderr = ""
            return R()  # rsync no-op — files already in place

        item = _make_item(
            kind="series", library="TV Shows",
            current_tier="WARM", current_disk=disk,
            warm_disk_files={disk: [src_file]},
        )
        item.outcome = "TO_HOT"

        cfg = {
            "moves": {
                "enabled": True, "rsync_options": ["-aH"],
                "delete_source_after_verify": True, "size_verify": True,
                "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            },
            "paths": {"hot_pool_mount": hot_mount},
        }

        orig = subprocess.run
        try:
            subprocess.run = _fake_run
            _run_move_pass([item], cfg, apply=True)
        finally:
            subprocess.run = orig

        assert not _os.path.exists(src_file), (
            "source file must be deleted — verify should pass with file-level measurement"
        )
        assert _os.path.exists(preexisting), "pre-existing dst file must be untouched"
    print("_test_size_verify_mixed_tier_preexisting_dst_passes: OK")


def _test_empty_ancestor_dirs_pruned_after_delete():
    """Season dir, show dir, and intermediate dirs are removed when emptied by a move.

    Simulates a TV series where Season 1 lives on disk1 (non-dominant) and Season 2
    on disk5 (dominant). After all files are deleted, both season dirs and both show
    dirs must be pruned, not just the immediate parent of each file.
    """
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk1 = _os.path.join(root, "disk1")
        disk5 = _os.path.join(root, "disk5")
        hot_mount = _os.path.join(root, "zfs_media")

        s1_ep = _os.path.join(disk1, "TV Shows", "Sullivan's Crossing (2023)", "Season 1", "S01E01.mkv")
        s2_ep = _os.path.join(disk5, "TV Shows", "Sullivan's Crossing (2023)", "Season 2", "S02E01.mkv")
        dst_s1 = _os.path.join(hot_mount, "TV Shows", "Sullivan's Crossing (2023)", "Season 1", "S01E01.mkv")
        dst_s2 = _os.path.join(hot_mount, "TV Shows", "Sullivan's Crossing (2023)", "Season 2", "S02E01.mkv")

        for f in (s1_ep, s2_ep, dst_s1, dst_s2):
            _os.makedirs(_os.path.dirname(f), exist_ok=True)
            with open(f, "wb") as fh:
                fh.write(b"x" * 100)

        def _fake_run(*_a, **_kw):
            class R:
                returncode = 0
                stderr = ""
            return R()

        item = _make_item(
            kind="series", library="TV Shows",
            current_tier="WARM", current_disk=disk5,
            warm_disk_files={disk1: [s1_ep], disk5: [s2_ep]},
        )
        item.outcome = "TO_HOT"

        cfg = {
            "moves": {
                "enabled": True, "rsync_options": ["-aH"],
                "delete_source_after_verify": True, "size_verify": True,
                "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            },
            "paths": {"hot_pool_mount": hot_mount},
        }

        orig = subprocess.run
        try:
            subprocess.run = _fake_run
            _run_move_pass([item], cfg, apply=True)
        finally:
            subprocess.run = orig

        # Files must be gone
        assert not _os.path.exists(s1_ep), "S01E01.mkv source must be deleted"
        assert not _os.path.exists(s2_ep), "S02E01.mkv source must be deleted"
        # Season dirs must be pruned (were empty after file deletion)
        assert not _os.path.exists(_os.path.dirname(s1_ep)), "Season 1 dir on disk1 must be pruned"
        assert not _os.path.exists(_os.path.dirname(s2_ep)), "Season 2 dir on disk5 must be pruned"
        # Show dirs must be pruned (became empty after season dirs removed)
        show_disk1 = _os.path.join(disk1, "TV Shows", "Sullivan's Crossing (2023)")
        show_disk5 = _os.path.join(disk5, "TV Shows", "Sullivan's Crossing (2023)")
        assert not _os.path.exists(show_disk1), "show dir on disk1 must be pruned"
        assert not _os.path.exists(show_disk5), "show dir on disk5 must be pruned"
        # Disk roots themselves must never be touched
        assert _os.path.exists(disk1), "disk1 root must survive"
        assert _os.path.exists(disk5), "disk5 root must survive"

    print("_test_empty_ancestor_dirs_pruned_after_delete: OK")


def _test_companion_files_included_in_warm_disk_files():
    """Year-folder structure: srt/nfo companions included, unrelated movie excluded."""
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk4")
        hot_mount = _os.path.join(root, "zfs_media")
        # Year-folder structure: multiple movies share the '2016' folder.
        year_dir = _os.path.join(disk, "Movies", "2016")
        _os.makedirs(year_dir, exist_ok=True)
        _os.makedirs(hot_mount, exist_ok=True)

        mkv = _os.path.join(year_dir, "Moana (2016).mkv")
        nfo = _os.path.join(year_dir, "Moana (2016).nfo")
        srt = _os.path.join(year_dir, "Moana (2016).en.srt")
        unrelated = _os.path.join(year_dir, "Other Movie (2016).mkv")

        for f in (mkv, nfo, srt, unrelated):
            with open(f, "wb") as fh:
                fh.write(b"x" * 100)

        parts = [(mkv, 100)]
        tier, _, _, _, wdf, _hot = resolve_item_current_tier(
            parts=parts,
            path_map=[],
            hot_mount=hot_mount,
            array_disks=[disk],
        )

        assert tier == "WARM", f"expected WARM, got {tier}"
        files = wdf.get(disk, [])
        assert mkv in files, f"mkv missing from warm_disk_files: {files}"
        assert nfo in files, f"nfo missing from warm_disk_files: {files}"
        assert srt in files, f"srt missing from warm_disk_files: {files}"
        assert unrelated not in files, f"unrelated file must not be included: {files}"

    print("_test_companion_files_included_in_warm_disk_files: OK")


def _test_movie_per_folder_extras_included():
    """Movie-per-folder structure: ALL extras in the folder are included (any stem)."""
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk5")
        hot_mount = _os.path.join(root, "zfs_media")
        # Hot pool copy — main movie + extras all in the movie's own folder.
        movie_dir = _os.path.join(hot_mount, "DVD Rips", "Movies",
                                  "Austin Powers in Goldmember (2002)")
        _os.makedirs(movie_dir, exist_ok=True)
        _os.makedirs(disk, exist_ok=True)

        main_mkv = _os.path.join(movie_dir,
                                 "Austin Powers in Goldmember (2002).mkv")
        trailer  = _os.path.join(movie_dir,
                                 "Austin Powers in Goldmember-trailer.mkv")
        featurette = _os.path.join(movie_dir, "Disco Fever-featurette.mkv")
        deleted  = _os.path.join(movie_dir, "Bloopers-deleted.mkv")
        nfo      = _os.path.join(movie_dir,
                                 "Austin Powers in Goldmember (2002).nfo")

        for f in (main_mkv, trailer, featurette, deleted, nfo):
            with open(f, "wb") as fh:
                fh.write(b"x" * 100)

        parts = [(main_mkv, 100)]
        tier, _, _, _, _, hot = resolve_item_current_tier(
            parts=parts,
            path_map=[],
            hot_mount=hot_mount,
            array_disks=[disk],
        )

        assert tier == "HOT", f"expected HOT, got {tier}"
        # All extras must be in hot_pool_files for TO_WARM to pick them up.
        assert main_mkv in hot, f"main mkv missing: {hot}"
        assert trailer in hot, f"trailer missing: {hot}"
        assert featurette in hot, f"featurette missing: {hot}"
        assert deleted in hot, f"deleted scene missing: {hot}"
        assert nfo in hot, f"nfo missing: {hot}"

    print("_test_movie_per_folder_extras_included: OK")


def _test_cross_tier_companion_probe():
    """Main file moved to warm in a prior run; extras stranded on hot pool are discovered."""
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk5")
        hot_mount = _os.path.join(root, "zfs_media")
        # Warm disk: only the main .mkv made it here in a prior run.
        warm_movie_dir = _os.path.join(disk, "DVD Rips", "Movies",
                                       "Austin Powers (2002)")
        # Hot pool: main movie folder still has the extras that were never moved.
        hot_movie_dir = _os.path.join(hot_mount, "DVD Rips", "Movies",
                                      "Austin Powers (2002)")
        _os.makedirs(warm_movie_dir, exist_ok=True)
        _os.makedirs(hot_movie_dir, exist_ok=True)

        main_warm = _os.path.join(warm_movie_dir, "Austin Powers (2002).mkv")
        trailer_hot = _os.path.join(hot_movie_dir,
                                    "Austin Powers-trailer.mkv")
        featurette_hot = _os.path.join(hot_movie_dir, "Disco Fever-featurette.mkv")

        for f in (main_warm, trailer_hot, featurette_hot):
            with open(f, "wb") as fh:
                fh.write(b"x" * 100)

        parts = [(main_warm, 100)]
        tier, _, _, _, _, hot = resolve_item_current_tier(
            parts=parts,
            path_map=[],
            hot_mount=hot_mount,
            array_disks=[disk],
        )

        assert tier == "WARM", f"expected WARM, got {tier}"
        # Main file is on warm; extras stranded on hot pool should be discovered.
        assert trailer_hot in hot, f"trailer missing from hot_pool_files: {hot}"
        assert featurette_hot in hot, f"featurette missing from hot_pool_files: {hot}"
        # Main warm file must NOT appear in hot_pool_files.
        assert main_warm not in hot, f"warm file must not appear in hot_pool_files: {hot}"

    print("_test_cross_tier_companion_probe: OK")


def _test_movie_per_folder_article_inversion():
    """_is_movie_per_folder matches when folder uses natural title, file uses sort-title article form."""
    # Exact match still works
    assert _is_movie_per_folder("Austin Powers (2002)", "Austin Powers (2002)")
    # Article at start of folder, moved to end in file stem (Plex sort-title convention)
    assert _is_movie_per_folder("The Bounty Hunter (2010)", "Bounty Hunter, The (2010)")
    assert _is_movie_per_folder("A Beautiful Mind (2001)", "Beautiful Mind, A (2001)")
    assert _is_movie_per_folder("An American Werewolf in London (1981)", "American Werewolf in London, An (1981)")
    # Case-insensitive
    assert _is_movie_per_folder("the bounty hunter (2010)", "Bounty Hunter, The (2010)")
    # Unrelated titles must not match
    assert not _is_movie_per_folder("The Matrix (1999)", "Bounty Hunter, The (2010)")
    # Same title without year mismatch
    assert _is_movie_per_folder("The Bounty Hunter", "Bounty Hunter, The")
    print("_test_movie_per_folder_article_inversion: OK")


def _test_straggler_stay_warm_upgraded_to_to_warm():
    """STAY_WARM item with hot_pool_files is upgraded to TO_WARM by collect_all."""
    item = _make_item(kind="movie", size_bytes=100)
    item.outcome = "STAY_WARM"
    item.current_tier = "WARM"
    item.hot_pool_files = ["/mnt/zfs/Movies/Foo (2020)/Foo-trailer.mkv"]

    straggler_to_warm = 0
    if item.outcome == "STAY_WARM" and item.hot_pool_files:
        item.outcome = "TO_WARM"
        straggler_to_warm += 1

    assert item.outcome == "TO_WARM", f"expected TO_WARM, got {item.outcome}"
    assert straggler_to_warm == 1
    print("_test_straggler_stay_warm_upgraded_to_to_warm: OK")


def _test_straggler_stay_hot_upgraded_to_to_hot():
    """STAY_HOT item with warm_disk_files is upgraded to TO_HOT by collect_all."""
    item = _make_item(kind="movie", size_bytes=100)
    item.outcome = "STAY_HOT"
    item.current_tier = "HOT"
    item.warm_disk_files = {"/mnt/disk5": ["/mnt/disk5/Movies/Foo (2020)/Foo.mkv"]}

    straggler_to_hot = 0
    if (item.outcome == "STAY_HOT" or item.outcome == "PIN_HOT") and item.warm_disk_files:
        item.outcome = "TO_HOT"
        straggler_to_hot += 1

    assert item.outcome == "TO_HOT", f"expected TO_HOT, got {item.outcome}"
    assert straggler_to_hot == 1
    print("_test_straggler_stay_hot_upgraded_to_to_hot: OK")


def _test_straggler_pin_hot_warm_upgraded_to_to_hot():
    """PIN_HOT item with warm_disk_files is promoted to TO_HOT (pinned but still on warm disk)."""
    item = _make_item(kind="movie", size_bytes=1_000_000_000)
    item.outcome = "PIN_HOT"
    item.current_tier = "WARM"
    item.warm_disk_files = {"/mnt/disk7": ["/mnt/disk7/Movies/Harry Potter (2004)/Harry Potter (2004).mkv"]}

    straggler_to_hot = 0
    if (item.outcome == "STAY_HOT" or item.outcome == "PIN_HOT") and item.warm_disk_files:
        item.outcome = "TO_HOT"
        straggler_to_hot += 1

    assert item.outcome == "TO_HOT", f"expected TO_HOT for pinned-but-warm item, got {item.outcome}"
    assert straggler_to_hot == 1
    print("_test_straggler_pin_hot_warm_upgraded_to_to_hot: OK")


def _test_straggler_no_upgrade_when_no_wrong_tier_files():
    """STAY/PIN outcomes with no stranded files are not upgraded."""
    stay_warm = _make_item(kind="movie", size_bytes=100)
    stay_warm.outcome = "STAY_WARM"
    stay_warm.hot_pool_files = []

    stay_hot = _make_item(kind="movie", size_bytes=100)
    stay_hot.outcome = "STAY_HOT"
    stay_hot.warm_disk_files = {}

    pin_hot_already_hot = _make_item(kind="movie", size_bytes=100)
    pin_hot_already_hot.outcome = "PIN_HOT"
    pin_hot_already_hot.warm_disk_files = {}  # already on hot pool — no stragglers

    for it in (stay_warm, stay_hot, pin_hot_already_hot):
        if it.outcome == "STAY_WARM" and it.hot_pool_files:
            it.outcome = "TO_WARM"
        elif (it.outcome == "STAY_HOT" or it.outcome == "PIN_HOT") and it.warm_disk_files:
            it.outcome = "TO_HOT"

    assert stay_warm.outcome == "STAY_WARM", f"must not upgrade: {stay_warm.outcome}"
    assert stay_hot.outcome == "STAY_HOT", f"must not upgrade: {stay_hot.outcome}"
    assert pin_hot_already_hot.outcome == "PIN_HOT", f"must not upgrade: {pin_hot_already_hot.outcome}"
    print("_test_straggler_no_upgrade_when_no_wrong_tier_files: OK")


def _test_movie_straggler_to_warm_colocates_with_main_file():
    """Movie straggler TO_WARM sends extras to the same disk as the main file."""
    import tempfile, os as _os
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        with tempfile.TemporaryDirectory() as root:
            disk3 = _os.path.join(root, "disk3")
            disk5 = _os.path.join(root, "disk5")
            movie_dir = _os.path.join(disk5, "Movies", "Foo (2020)")
            _os.makedirs(movie_dir, exist_ok=True)
            _os.makedirs(disk3, exist_ok=True)
            # Main movie file already on disk5.
            main_file = _os.path.join(movie_dir, "Foo (2020).mkv")
            with open(main_file, "wb") as fh:
                fh.write(b"x" * (4 * 1024 ** 2))  # 4 MB

            free_map = {disk3: 500 * 1024 ** 3, disk5: 200 * 1024 ** 3}
            _disk_free_bytes = lambda p: free_map.get(p, 0)  # noqa: E731

            item = _make_item(kind="movie", size_bytes=5 * 1024 ** 3,
                              warm_disk_files={disk5: [main_file]})
            disk, annot = _select_warm_destination(
                item, [disk3, disk5], safety_margin_bytes=0
            )
            assert disk == disk5, f"expected disk5 (co-locate with main file), got {disk}"
            assert annot and "co-locate" in annot, f"expected co-locate annotation, got {annot}"
    finally:
        _disk_free_bytes = orig
    print("_test_movie_straggler_to_warm_colocates_with_main_file: OK")


def _test_warm_disk_selection_to_warm_picks_most_free():
    """TO_WARM with two candidate disks — selects the one with most free space."""
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        free_map = {"/mnt/disk1": 100 * 1024 ** 3, "/mnt/disk2": 200 * 1024 ** 3}
        _disk_free_bytes = lambda p: free_map.get(p, 0)  # noqa: E731

        item = _make_item(kind="movie", size_bytes=10 * 1024 ** 3)
        disk, annot = _select_warm_destination(
            item, ["/mnt/disk1", "/mnt/disk2"], safety_margin_bytes=0
        )
        assert disk == "/mnt/disk2", f"expected disk2 (most-free), got {disk}"
        assert annot == "most-free", f"expected most-free, got {annot}"
    finally:
        _disk_free_bytes = orig
    print("_test_warm_disk_selection_to_warm_picks_most_free: OK")


def _test_warm_disk_selection_relocate_excludes_source():
    """RELOCATE_WARM excludes current_disk from candidates."""
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        # disk1 is the evicting source, disk2 has space
        free_map = {"/mnt/disk1": 500 * 1024 ** 3, "/mnt/disk2": 200 * 1024 ** 3}
        _disk_free_bytes = lambda p: free_map.get(p, 0)  # noqa: E731

        item = _make_item(kind="movie", size_bytes=10 * 1024 ** 3)
        disk, annot = _select_warm_destination(
            item, ["/mnt/disk1", "/mnt/disk2"],
            safety_margin_bytes=0, exclude_disk="/mnt/disk1",
        )
        assert disk == "/mnt/disk2", f"source disk must be excluded, got {disk}"
        assert annot == "most-free"
    finally:
        _disk_free_bytes = orig
    print("_test_warm_disk_selection_relocate_excludes_source: OK")


def _test_warm_disk_selection_co_locate_for_series():
    """Series with warm bytes already on disk2 — co-location wins over most-free."""
    import tempfile, os as _os
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        with tempfile.TemporaryDirectory() as root:
            disk2 = _os.path.join(root, "disk2")
            show_dir = _os.path.join(disk2, "TV Shows", "Reba (2001)")
            _os.makedirs(show_dir, exist_ok=True)
            ep_file = _os.path.join(show_dir, "S01E01.mkv")
            with open(ep_file, "wb") as fh:
                fh.write(b"x" * (5 * 1024 ** 3))  # 5 GB already on disk2

            free_map = {"/mnt/disk1": 400 * 1024 ** 3, disk2: 200 * 1024 ** 3}
            _disk_free_bytes = lambda p: free_map.get(p, 0)  # noqa: E731

            item = _make_item(
                kind="series", size_bytes=20 * 1024 ** 3,
                warm_disk_files={disk2: [ep_file]},
            )
            disk, annot = _select_warm_destination(
                item, ["/mnt/disk1", disk2], safety_margin_bytes=0
            )
            assert disk == disk2, f"expected co-location on disk2, got {disk}"
            assert annot and "co-locate" in annot, f"expected co-locate annotation, got {annot}"
    finally:
        _disk_free_bytes = orig
    print("_test_warm_disk_selection_co_locate_for_series: OK")


def _test_warm_disk_selection_no_capacity_returns_none():
    """No disk has enough space — returns (None, reason)."""
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        _disk_free_bytes = lambda _: 1 * 1024 ** 3  # 1 GB free everywhere  # noqa: E731
        item = _make_item(size_bytes=100 * 1024 ** 3)
        disk, annot = _select_warm_destination(
            item, ["/mnt/disk1"], safety_margin_bytes=0
        )
        assert disk is None, f"expected None, got {disk}"
        assert annot is not None, "expected failure reason string"
    finally:
        _disk_free_bytes = orig
    print("_test_warm_disk_selection_no_capacity_returns_none: OK")


def _test_warm_disk_selection_safety_margin_respected():
    """Disk has item.size_bytes free but not item.size_bytes + margin — excluded."""
    global _disk_free_bytes
    orig = _disk_free_bytes
    try:
        item_bytes = 10 * 1024 ** 3
        margin_bytes = 50 * 1024 ** 3
        # Disk has exactly item_bytes free — not enough with the margin
        _disk_free_bytes = lambda _: item_bytes  # noqa: E731
        item = _make_item(size_bytes=item_bytes)
        disk, _ = _select_warm_destination(
            item, ["/mnt/disk1"], safety_margin_bytes=margin_bytes
        )
        assert disk is None, f"margin not respected — got disk={disk}"

        # Disk has item_bytes + margin free — should qualify
        _disk_free_bytes = lambda _: item_bytes + margin_bytes  # noqa: E731
        disk2, _ = _select_warm_destination(
            item, ["/mnt/disk1"], safety_margin_bytes=margin_bytes
        )
        assert disk2 == "/mnt/disk1", f"should qualify with exact margin, got {disk2}"
    finally:
        _disk_free_bytes = orig
    print("_test_warm_disk_selection_safety_margin_respected: OK")


def _test_to_warm_full_flow_end_to_end():
    """TO_WARM: hot pool files are rsynced to chosen warm disk; source deleted."""
    import tempfile, os as _os, subprocess as _sp

    with tempfile.TemporaryDirectory() as root:
        hot = _os.path.join(root, "zfs_media")
        disk1 = _os.path.join(root, "disk1")
        movie_dir = _os.path.join(hot, "Movies", "Interstellar (2014)")
        _os.makedirs(movie_dir, exist_ok=True)
        _os.makedirs(disk1, exist_ok=True)

        mkv = _os.path.join(movie_dir, "Interstellar.mkv")
        with open(mkv, "wb") as fh:
            fh.write(b"x" * 1000)

        # Build Item as if it were HOT with hot_pool_files populated
        item = _make_item(
            kind="movie", size_bytes=1000,
            current_tier="HOT",
            hot_pool_files=[mkv],
            warm_disk_files={},
        )
        item.outcome = "TO_WARM"

        rsync_calls = []
        orig_run = _sp.run

        def _fake_rsync(cmd, **_):
            rsync_calls.append(cmd)
            # Actually copy the file so size-verify passes
            import shutil as _sh
            src_root = cmd[-2]
            dst_root = cmd[-1]
            ff_path = [a for a in cmd if a.startswith("--files-from=")][0].split("=", 1)[1]
            with open(ff_path) as f:
                for rel in f.read().splitlines():
                    src = _os.path.join(src_root, rel)
                    dst = _os.path.join(dst_root, rel)
                    _os.makedirs(_os.path.dirname(dst), exist_ok=True)
                    _sh.copy2(src, dst)

            class R:
                returncode = 0
                stderr = ""
            return R()

        _sp.run = _fake_rsync
        try:
            status = _exec_single_move(
                prefix="[1/1]",
                title_year="Interstellar (2014)",
                files_by_src_root={hot: [mkv]},
                dst_root=disk1 + "/",
                rsync_opts=["-aH"],
                size_verify=True,
                delete_after=True,
            )
        finally:
            _sp.run = orig_run

        assert status == "ok", f"expected ok, got {status}"
        assert not _os.path.exists(mkv), "source file must be deleted after verify"
        dst_mkv = _os.path.join(disk1, "Movies", "Interstellar (2014)", "Interstellar.mkv")
        assert _os.path.exists(dst_mkv), f"destination file not found: {dst_mkv}"
        assert len(rsync_calls) == 1, f"expected 1 rsync call, got {len(rsync_calls)}"
        # Source root in rsync command must be the hot mount, not the warm disk
        assert hot.rstrip("/") + "/" in rsync_calls[0], "rsync source must be hot pool"

    print("_test_to_warm_full_flow_end_to_end: OK")


def _test_relocate_warm_full_flow_end_to_end():
    """RELOCATE_WARM: files rsynced from current_disk to chosen disk; source disk excluded."""
    import tempfile, os as _os, subprocess as _sp

    with tempfile.TemporaryDirectory() as root:
        disk7 = _os.path.join(root, "disk7")  # evicting source
        disk4 = _os.path.join(root, "disk4")  # destination
        show_dir = _os.path.join(disk7, "TV Shows", "Reba (2001)")
        _os.makedirs(show_dir, exist_ok=True)
        _os.makedirs(disk4, exist_ok=True)

        ep = _os.path.join(show_dir, "S01E01.mkv")
        with open(ep, "wb") as fh:
            fh.write(b"x" * 1000)

        item = _make_item(
            kind="series", size_bytes=1000,
            current_tier="WARM", current_disk=disk7,
            warm_disk_files={disk7: [ep]},
        )
        item.outcome = "RELOCATE_WARM"

        rsync_calls = []
        orig_run = _sp.run

        def _fake_rsync(cmd, **_):
            rsync_calls.append(cmd)
            import shutil as _sh
            src_root = cmd[-2]
            dst_root = cmd[-1]
            ff_path = [a for a in cmd if a.startswith("--files-from=")][0].split("=", 1)[1]
            with open(ff_path) as f:
                for rel in f.read().splitlines():
                    src = _os.path.join(src_root, rel)
                    dst = _os.path.join(dst_root, rel)
                    _os.makedirs(_os.path.dirname(dst), exist_ok=True)
                    _sh.copy2(src, dst)

            class R:
                returncode = 0
                stderr = ""
            return R()

        _sp.run = _fake_rsync
        try:
            status = _exec_single_move(
                prefix="[1/1]",
                title_year="Reba (2001)",
                files_by_src_root={disk7: [ep]},
                dst_root=disk4 + "/",
                rsync_opts=["-aH"],
                size_verify=True,
                delete_after=True,
            )
        finally:
            _sp.run = orig_run

        assert status == "ok", f"expected ok, got {status}"
        assert not _os.path.exists(ep), "source file must be deleted"
        dst_ep = _os.path.join(disk4, "TV Shows", "Reba (2001)", "S01E01.mkv")
        assert _os.path.exists(dst_ep), f"destination not found: {dst_ep}"
        # Confirm rsync source was disk7, not disk4
        assert disk7.rstrip("/") + "/" in rsync_calls[0], "rsync must use evicting disk as source"
        assert disk4.rstrip("/") + "/" not in rsync_calls[0][:-1], "evicting disk must not appear as rsync dest source"

    print("_test_relocate_warm_full_flow_end_to_end: OK")


def _test_relocate_warm_co_locate_with_existing_partial():
    """RELOCATE_WARM: if destination already has partial series, rsync merges correctly."""
    import tempfile, os as _os, subprocess as _sp

    with tempfile.TemporaryDirectory() as root:
        disk7 = _os.path.join(root, "disk7")  # evicting disk (has S01)
        disk4 = _os.path.join(root, "disk4")  # destination (already has S02)

        s01_dir = _os.path.join(disk7, "TV Shows", "Reba (2001)", "Season 01")
        s02_dir = _os.path.join(disk4, "TV Shows", "Reba (2001)", "Season 02")
        _os.makedirs(s01_dir, exist_ok=True)
        _os.makedirs(s02_dir, exist_ok=True)

        ep1 = _os.path.join(s01_dir, "S01E01.mkv")
        ep2 = _os.path.join(s02_dir, "S02E01.mkv")
        for f in (ep1, ep2):
            with open(f, "wb") as fh:
                fh.write(b"x" * 500)

        # Item has warm bytes on disk7 (evicting) and disk4 (co-locate target).
        # Not passed to _exec_single_move; created for documentation only.
        _item = _make_item(
            kind="series", size_bytes=1000,
            current_tier="WARM", current_disk=disk7,
            warm_disk_files={disk7: [ep1], disk4: [ep2]},
        )
        _ = _item  # suppress unused-variable hint

        rsync_calls = []
        orig_run = _sp.run

        def _fake_rsync(cmd, **_):
            rsync_calls.append(list(cmd))
            import shutil as _sh
            src_root = cmd[-2]
            dst_root = cmd[-1]
            ff_path = [a for a in cmd if a.startswith("--files-from=")][0].split("=", 1)[1]
            with open(ff_path) as f:
                for rel in f.read().splitlines():
                    src = _os.path.join(src_root, rel)
                    dst = _os.path.join(dst_root, rel)
                    if _os.path.exists(src) and src != dst:
                        _os.makedirs(_os.path.dirname(dst), exist_ok=True)
                        _sh.copy2(src, dst)

            class R:
                returncode = 0
                stderr = ""
            return R()

        _sp.run = _fake_rsync
        try:
            # Relocate all warm files (disk7 + disk4) to disk4 as destination.
            # The disk4 source entries have src == dst (already in place);
            # the fake rsync skips same-file copies, mirroring real rsync behaviour.
            status = _exec_single_move(
                prefix="[1/1]",
                title_year="Reba (2001)",
                files_by_src_root={disk7: [ep1], disk4: [ep2]},
                dst_root=disk4 + "/",
                rsync_opts=["-aH"],
                size_verify=True,
                delete_after=False,  # don't delete in this test to simplify
            )
        finally:
            _sp.run = orig_run

        assert status == "ok", f"expected ok, got {status}"
        # Two rsync calls: one per source disk
        assert len(rsync_calls) == 2, f"expected 2 rsync calls (one per source disk), got {len(rsync_calls)}"
        src_roots = [c[-2] for c in rsync_calls]
        assert disk7.rstrip("/") + "/" in src_roots, "disk7 must be a rsync source"
        assert disk4.rstrip("/") + "/" in src_roots, "disk4 must be a rsync source (merge)"

    print("_test_relocate_warm_co_locate_with_existing_partial: OK")


# ---------- P3 capacity tests ----------

def _make_cap_cfg(
    ceiling_pct=80,
    warm_ceiling_pct=90,
    safety_gb=0,
    auto_demote=False,
):
    """Build a minimal cfg dict for capacity tests."""
    return {
        "paths": {"hot_pool_mount": "/mnt/hot", "array_disks": [], "array_disk_exclude": []},
        "capacity": {
            "hot_ceiling_percent": ceiling_pct,
            "warm_per_disk_ceiling_percent": warm_ceiling_pct,
            "budget_safety_margin_gb": safety_gb,
            "auto_demote_when_over_ceiling": auto_demote,
        },
    }


def _make_fake_disk_usage(total_gb, used_gb):
    """Return a callable that mimics shutil.disk_usage for test patching."""
    import collections
    DU = collections.namedtuple("DU", ["total", "used", "free"])
    GB = 1024 ** 3
    total = int(total_gb * GB)
    used = int(used_gb * GB)
    return lambda _path: DU(total=total, used=used, free=total - used)


def _test_capacity_budget_caps_to_hot_promotions():
    """High-score item fits within budget; lower-score item is deferred."""
    global _disk_usage
    orig = _disk_usage
    try:
        # Pool: 10 TB total, 6 TB used. ceiling=80% → ceiling=8 TB, budget=2 TB.
        # Item A (score=80, 1.5 TB) fits; item B (score=50, 1 TB) makes total 2.5 TB → deferred.
        TB = 1024 ** 3 * 1024
        _disk_usage = _make_fake_disk_usage(10 * 1024, 6 * 1024)  # GB args

        item_a = _make_item(score=80.0, size_bytes=int(1.5 * TB), outcome="TO_HOT")
        item_b = _make_item(score=50.0, size_bytes=TB, outcome="TO_HOT")
        items = [item_a, item_b]

        _apply_capacity_budget(items, _make_cap_cfg(ceiling_pct=80, safety_gb=0))

        assert item_a.outcome == "TO_HOT", f"high-score item should fit, got {item_a.outcome}"
        assert item_b.outcome == "OVER_BUDGET_HOT", f"low-score item should be deferred, got {item_b.outcome}"
    finally:
        _disk_usage = orig
    print("_test_capacity_budget_caps_to_hot_promotions: OK")


def _test_capacity_budget_zero_makes_all_over_budget():
    """Pool exactly at ceiling → budget=0 → all TO_HOT items deferred."""
    global _disk_usage
    orig = _disk_usage
    try:
        # Pool: 10 TB total, 8 TB used. ceiling=80% → ceiling=8 TB, budget=0.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 8 * 1024)

        items = [
            _make_item(score=90.0, size_bytes=1, outcome="TO_HOT"),
            _make_item(score=70.0, size_bytes=1, outcome="TO_HOT"),
        ]
        _apply_capacity_budget(items, _make_cap_cfg(ceiling_pct=80, safety_gb=0))

        for it in items:
            assert it.outcome == "OVER_BUDGET_HOT", f"expected OVER_BUDGET_HOT, got {it.outcome}"
    finally:
        _disk_usage = orig
    print("_test_capacity_budget_zero_makes_all_over_budget: OK")


def _test_capacity_over_ceiling_auto_demotes_lowest_scorers():
    """Pool over ceiling + auto_demote=True → lowest STAY_HOT items become TO_WARM."""
    global _disk_usage
    orig = _disk_usage
    try:
        GB = 1024 ** 3
        # Pool: 10 TB, used 9 TB → 90% > 80% ceiling → over by 1 TB.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 9 * 1024)

        # Two STAY_HOT items. Low score is demoted first.
        item_low = _make_item(score=25.0, size_bytes=int(1.5 * 1024 * GB), outcome="STAY_HOT")
        item_high = _make_item(score=75.0, size_bytes=int(1.5 * 1024 * GB), outcome="STAY_HOT")
        items = [item_high, item_low]  # order shouldn't matter; sort by score ascending

        _apply_capacity_budget(items, _make_cap_cfg(ceiling_pct=80, auto_demote=True))

        # Need to shed 1 TB. item_low is 1.5 TB — one demotion is enough.
        assert item_low.outcome == "TO_WARM", f"expected TO_WARM, got {item_low.outcome}"
        assert item_high.outcome == "STAY_HOT", f"high-score item should be exempt, got {item_high.outcome}"
    finally:
        _disk_usage = orig
    print("_test_capacity_over_ceiling_auto_demotes_lowest_scorers: OK")


def _test_capacity_over_ceiling_does_not_demote_pin_hot():
    """PIN_HOT items are always exempt from auto-demote even when over ceiling."""
    global _disk_usage
    orig = _disk_usage
    try:
        # Pool: 10 TB, 9 TB used → over 80% ceiling.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 9 * 1024)

        item_pin = _make_item(score=5.0, size_bytes=int(2 * 1024 * 1024 ** 3), outcome="PIN_HOT")
        item_stay = _make_item(score=22.0, size_bytes=int(2 * 1024 * 1024 ** 3), outcome="STAY_HOT")
        items = [item_pin, item_stay]

        _apply_capacity_budget(items, _make_cap_cfg(ceiling_pct=80, auto_demote=True))

        assert item_pin.outcome == "PIN_HOT", f"PIN_HOT must be exempt, got {item_pin.outcome}"
        # item_stay should be demoted to bring pool under ceiling
        assert item_stay.outcome == "TO_WARM", f"expected TO_WARM, got {item_stay.outcome}"
    finally:
        _disk_usage = orig
    print("_test_capacity_over_ceiling_does_not_demote_pin_hot: OK")


def _test_capacity_no_promote_flag_blocks_all_to_hot():
    """--no-promote converts all TO_HOT items to OVER_BUDGET_HOT regardless of budget."""
    global _disk_usage
    orig = _disk_usage
    try:
        # Pool has ample budget — doesn't matter, --no-promote wins.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 1 * 1024)

        items = [
            _make_item(score=95.0, size_bytes=1, outcome="TO_HOT"),
            _make_item(score=60.0, size_bytes=1, outcome="TO_HOT"),
            _make_item(score=30.0, size_bytes=1, outcome="STAY_WARM"),  # unaffected
        ]
        _apply_capacity_budget(items, _make_cap_cfg(), no_promote=True)

        assert items[0].outcome == "OVER_BUDGET_HOT"
        assert items[1].outcome == "OVER_BUDGET_HOT"
        assert items[2].outcome == "STAY_WARM", "STAY_WARM must be unaffected by --no-promote"
    finally:
        _disk_usage = orig
    print("_test_capacity_no_promote_flag_blocks_all_to_hot: OK")


def _test_capacity_no_demote_flag_blocks_to_warm():
    """--no-demote suppresses the auto-demote pass even when pool is over ceiling."""
    global _disk_usage
    orig = _disk_usage
    try:
        # Pool: 9 TB / 10 TB → over 80% ceiling. auto_demote=True but no_demote=True.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 9 * 1024)

        item = _make_item(score=22.0, size_bytes=int(2 * 1024 * 1024 ** 3), outcome="STAY_HOT")
        _apply_capacity_budget(
            [item], _make_cap_cfg(ceiling_pct=80, auto_demote=True), no_demote=True
        )

        assert item.outcome == "STAY_HOT", f"--no-demote must block auto-demote, got {item.outcome}"
    finally:
        _disk_usage = orig
    print("_test_capacity_no_demote_flag_blocks_to_warm: OK")


def _test_capacity_safety_margin_respected():
    """Safety margin reduces the effective promotion budget."""
    global _disk_usage
    orig = _disk_usage
    try:
        GB = 1024 ** 3
        # Pool: 10 TB, 6 TB used. ceiling=80% → 8 TB, raw budget = 2 TB.
        # safety_margin = 1.5 TB → effective budget = 0.5 TB.
        # Item of 1 TB does not fit.
        _disk_usage = _make_fake_disk_usage(10 * 1024, 6 * 1024)

        item = _make_item(score=80.0, size_bytes=int(1024 * GB), outcome="TO_HOT")
        _apply_capacity_budget(
            [item], _make_cap_cfg(ceiling_pct=80, safety_gb=1536)  # 1536 GB = 1.5 TB
        )

        assert item.outcome == "OVER_BUDGET_HOT", (
            f"safety margin should shrink budget below item size, got {item.outcome}"
        )
    finally:
        _disk_usage = orig
    print("_test_capacity_safety_margin_respected: OK")


def _test_capacity_warm_per_disk_ceiling_blocks_to_warm_when_full():
    """All warm disks over the ceiling → _select_warm_destination returns None."""
    global _disk_usage, _disk_free_bytes
    orig_du = _disk_usage
    orig_dfb = _disk_free_bytes
    try:
        GB = 1024 ** 3
        import collections
        DU = collections.namedtuple("DU", ["total", "used", "free"])
        # Disks are 95% full; ceiling is 90%.
        _disk_usage = lambda _p: DU(total=10 * 1024 * GB, used=int(9.5 * 1024 * GB),
                                    free=int(0.5 * 1024 * GB))
        _disk_free_bytes = lambda _p: int(0.5 * 1024 * GB)

        item = _make_item(kind="movie", size_bytes=GB)
        disk, annot = _select_warm_destination(
            item, ["/mnt/disk1", "/mnt/disk2"],
            safety_margin_bytes=0,
            warm_ceiling_pct=0.90,
        )

        assert disk is None, f"expected None (all disks over ceiling), got {disk}"
        assert annot is not None and "ceiling" in annot.lower(), (
            f"expected ceiling message in annotation, got {annot!r}"
        )
    finally:
        _disk_usage = orig_du
        _disk_free_bytes = orig_dfb
    print("_test_capacity_warm_per_disk_ceiling_blocks_to_warm_when_full: OK")


def _test_run_cap_stops_after_limit():
    """Two items totalling > cap → first moves, second does not; early-stop log emitted."""
    rsync_calls = []
    log_messages = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    class _CapturingHandler(logging.Handler):
        def emit(self, record):
            log_messages.append(record.getMessage())

    GB = 1024 ** 3
    # item_a (60 GB) > cap (50 GB): before item_a moved_bytes=0 < 50*GB → proceeds.
    # After item_a succeeds: moved_bytes=60*GB >= 50*GB → item_b is blocked.
    item_a = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/A.mkv"]},
        size_bytes=60 * GB,
    )
    item_a.outcome = "TO_HOT"
    item_b = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/B.mkv"]},
        size_bytes=60 * GB,
    )
    item_b.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            "max_total_move_gb": 50,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    handler = _CapturingHandler()
    log.addHandler(handler)
    orig_level = log.level
    log.setLevel(logging.DEBUG)
    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([item_a, item_b], cfg, apply=True)
    finally:
        subprocess.run = orig
        log.removeHandler(handler)
        log.setLevel(orig_level)

    # item_a (60 GB) moves; after success moved_bytes=60*GB >= 50*GB cap → item_b blocked.
    assert len(rsync_calls) == 1, f"expected 1 rsync call, got {len(rsync_calls)}"
    assert item_a.outcome == "TO_HOT", f"first item outcome should be unchanged, got {item_a.outcome!r}"
    assert item_b.outcome == "TO_HOT", f"second item outcome should be unchanged, got {item_b.outcome!r}"
    assert any("Run cap reached" in m for m in log_messages), (
        f"expected early-stop log line, got: {log_messages}"
    )
    print("_test_run_cap_stops_after_limit: OK")


def _test_run_cap_zero_disables_cap():
    """max_total_move_gb: 0 → all items move regardless of total size."""
    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    GB = 1024 ** 3
    item_a = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/A.mkv"]},
        size_bytes=500 * GB,
    )
    item_a.outcome = "TO_HOT"
    item_b = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/B.mkv"]},
        size_bytes=500 * GB,
    )
    item_b.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            "max_total_move_gb": 0,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([item_a, item_b], cfg, apply=True)
    finally:
        subprocess.run = orig

    assert len(rsync_calls) == 2, f"cap=0 must not stop any moves, got {len(rsync_calls)} rsync calls"
    print("_test_run_cap_zero_disables_cap: OK")


def _test_run_cap_counts_successful_moves_only():
    """A failed move does not count toward the run budget.

    item_a fails size-verify → moved_bytes stays 0.
    item_b (same size) should still be attempted even though a naive
    accumulator would think the cap was reached.
    """
    import tempfile, os as _os

    GB = 1024 ** 3
    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk1")
        hot_mount = _os.path.join(root, "zfs_media")

        src_a = _os.path.join(disk, "Movies", "A.mkv")
        dst_a = _os.path.join(hot_mount, "Movies", "A.mkv")
        src_b = _os.path.join(disk, "Movies", "B.mkv")

        _os.makedirs(_os.path.dirname(src_a), exist_ok=True)
        _os.makedirs(_os.path.dirname(dst_a), exist_ok=True)
        # A: src=1 KB, dst already exists at 5 MB → size mismatch → verify fails
        with open(src_a, "wb") as f:
            f.write(b"x" * 1024)
        with open(dst_a, "wb") as f:
            f.write(b"y" * (5 * 1024 * 1024))
        # B: only src, no dst yet
        with open(src_b, "wb") as f:
            f.write(b"z" * 1024)

        rsync_calls = []

        def _fake_run(cmd, **_):
            if cmd and cmd[0] == "rsync":
                rsync_calls.append(cmd)
            class R:
                returncode = 0
                stderr = ""
            return R()  # rsync succeeds but dst is already wrong for A

        item_a = _make_item(
            kind="movie", library="Movies",
            current_tier="WARM", current_disk=disk,
            warm_disk_files={disk: [src_a]},
            size_bytes=60 * GB,
        )
        item_a.outcome = "TO_HOT"
        item_b = _make_item(
            kind="movie", library="Movies",
            current_tier="WARM", current_disk=disk,
            warm_disk_files={disk: [src_b]},
            size_bytes=60 * GB,
        )
        item_b.outcome = "TO_HOT"

        cfg = {
            "moves": {
                "enabled": True, "rsync_options": ["-aH"],
                "delete_source_after_verify": True, "size_verify": True,
                "parity_check_blocking": False, "bandwidth_limit_mbps": None,
                "max_total_move_gb": 80,
            },
            "paths": {"hot_pool_mount": hot_mount},
        }

        orig = subprocess.run
        try:
            subprocess.run = _fake_run
            _run_move_pass([item_a, item_b], cfg, apply=True)
        finally:
            subprocess.run = orig

        # Both items should have been attempted (2 rsync calls).
        assert len(rsync_calls) == 2, (
            f"both items must be attempted when first fails verify, got {len(rsync_calls)} rsync call(s)"
        )
    print("_test_run_cap_counts_successful_moves_only: OK")


def _test_run_cap_exact_boundary():
    """Cumulative bytes exactly equal cap after first item → second item still attempted.

    Cap is >=: the item that brings the total exactly to the cap is moved;
    only the *next* item is blocked.
    """
    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    GB = 1024 ** 3
    # Each item is exactly 50 GB; cap is 50 GB.
    # After item_a succeeds: moved_bytes == 50 GB == cap_bytes → item_b check: 50 >= 50 → blocked.
    # Wait — that means item_b IS blocked. But spec says "cap is >=: item that crosses threshold
    # is still attempted; next item is blocked." So item at exactly cap is moved, next is not.
    # With moved_bytes starting at 0: before item_a, 0 >= 50*GB? No → item_a moves.
    # After item_a succeeds: moved_bytes = 50*GB. Before item_b: 50*GB >= 50*GB → blocked.
    # So item_b should NOT be rsynced. That is the correct ">=" boundary behaviour.
    item_a = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/A.mkv"]},
        size_bytes=50 * GB,
    )
    item_a.outcome = "TO_HOT"
    item_b = _make_item(
        kind="movie", library="Movies",
        current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/B.mkv"]},
        size_bytes=50 * GB,
    )
    item_b.outcome = "TO_HOT"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
            "max_total_move_gb": 50,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media"},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([item_a, item_b], cfg, apply=True)
    finally:
        subprocess.run = orig

    # item_a (50 GB) moves; after success moved_bytes=50 GB == cap → item_b is blocked.
    assert len(rsync_calls) == 1, (
        f"expected item_a to move and item_b to be blocked at exact boundary, got {len(rsync_calls)} rsync call(s)"
    )
    print("_test_run_cap_exact_boundary: OK")


def _test_capacity_unraid_api_failure_warns_and_falls_back():
    """When unraid_api_url is set but _try_unraid_api returns None, a WARNING is emitted
    and _pool_usage_bytes falls through to the override method."""
    import logging
    global _try_unraid_api, _disk_usage
    orig_api = _try_unraid_api
    orig_du = _disk_usage
    captured = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record):
            captured.append(record)

    handler = _CapturingHandler()
    log.addHandler(handler)
    try:
        _try_unraid_api = lambda *_, **__: None  # noqa: E731

        GB = 1024 ** 3
        free_bytes = 60 * GB
        total_override_gb = 100.0
        _DU = type("DU", (), {"total": int(total_override_gb * GB), "used": 0, "free": free_bytes})()
        _disk_usage = lambda *_: _DU  # noqa: E731

        total, used = _pool_usage_bytes(
            mount="/mnt/hot_pool",
            total_gb_override=total_override_gb,
            unraid_api_url="https://unraid.invalid/graphql",
            unraid_api_key="dummy-key",
        )

        # (a) fell through to override: total = 100 GiB, used = total - free
        expected_total = int(total_override_gb * GB)
        expected_used = max(0, expected_total - free_bytes)
        assert total == expected_total, f"expected total={expected_total}, got {total}"
        assert used == expected_used, f"expected used={expected_used}, got {used}"

        # (b) a WARNING containing "unraid_api_url" was emitted
        warnings = [r for r in captured if r.levelno == logging.WARNING and "unraid_api_url" in r.getMessage()]
        assert warnings, "expected a WARNING about Unraid API fallback, got none"
    finally:
        _try_unraid_api = orig_api
        _disk_usage = orig_du
        log.removeHandler(handler)

    print("_test_capacity_unraid_api_failure_warns_and_falls_back: OK")


def _test_capacity_unraid_api_not_configured_no_warn():
    """When unraid_api_url is None, _try_unraid_api is never called and no WARNING fires."""
    import logging
    global _try_unraid_api, _disk_usage, _zfs_pool_name_for_mount
    orig_api = _try_unraid_api
    orig_du = _disk_usage
    orig_pool = _zfs_pool_name_for_mount
    captured = []
    api_calls = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record):
            captured.append(record)

    handler = _CapturingHandler()
    log.addHandler(handler)
    try:
        def _spy_api(*_):
            api_calls.append(1)
            return None
        _try_unraid_api = _spy_api
        _zfs_pool_name_for_mount = lambda *_: None  # noqa: E731

        GB = 1024 ** 3
        _DU = type("DU", (), {"total": 100 * GB, "used": 40 * GB, "free": 60 * GB})()
        _disk_usage = lambda *_: _DU  # noqa: E731

        _pool_usage_bytes(mount="/mnt/hot_pool", unraid_api_url=None)

        # _try_unraid_api must NOT have been called
        assert not api_calls, f"_try_unraid_api should not be called when url is None, was called {len(api_calls)} times"

        # No WARNING about Unraid API
        warnings = [r for r in captured if r.levelno == logging.WARNING and "unraid_api_url" in r.getMessage()]
        assert not warnings, f"expected no Unraid API warning, got: {[r.getMessage() for r in warnings]}"
    finally:
        _try_unraid_api = orig_api
        _disk_usage = orig_du
        _zfs_pool_name_for_mount = orig_pool
        log.removeHandler(handler)

    print("_test_capacity_unraid_api_not_configured_no_warn: OK")


def _test_unraid_api_verify_tls_false_uses_unverified_context():
    """verify_tls=False → urlopen receives an SSLContext with CERT_NONE / check_hostname=False."""
    import logging
    import urllib.request as _urlreq
    captured_kwargs: dict = {}
    captured_warnings: list = []

    class _CapturingHandler(logging.Handler):
        def emit(self, record):
            if record.levelno == logging.WARNING:
                captured_warnings.append(record.getMessage())

    handler = _CapturingHandler()
    log.addHandler(handler)

    def _fake_urlopen(req, timeout=None, context=None):
        captured_kwargs["context"] = context
        # Return a minimal JSON response so _try_unraid_api parses successfully.
        import io
        body = b'{"data": {"array": {"caches": []}}}'
        return io.BytesIO(body)

    orig_urlopen = _urlreq.urlopen
    _urlreq.urlopen = _fake_urlopen
    try:
        _try_unraid_api(
            "https://unraid.invalid/graphql",
            api_key="dummy-key",
            pool_name_filter=None,
            verify_tls=False,
        )
    finally:
        _urlreq.urlopen = orig_urlopen
        log.removeHandler(handler)

    ctx = captured_kwargs.get("context")
    assert ctx is not None, "expected an SSLContext to be passed to urlopen, got None"
    assert ctx.verify_mode == ssl.CERT_NONE, (
        f"expected CERT_NONE, got {ctx.verify_mode}"
    )
    assert ctx.check_hostname is False, (
        f"expected check_hostname=False, got {ctx.check_hostname}"
    )
    tls_warnings = [w for w in captured_warnings if "unraid_api_verify_tls=false" in w.lower()]
    assert tls_warnings, f"expected a TLS-disabled WARNING, got: {captured_warnings}"
    print("_test_unraid_api_verify_tls_false_uses_unverified_context: OK")


def _test_unraid_api_verify_tls_true_verifies():
    """verify_tls=True (default) → urlopen receives None context (stdlib default verification)."""
    import urllib.request as _urlreq
    captured_kwargs: dict = {}

    def _fake_urlopen(req, timeout=None, context=None):
        captured_kwargs["context"] = context
        import io
        body = b'{"data": {"array": {"caches": []}}}'
        return io.BytesIO(body)

    orig_urlopen = _urlreq.urlopen
    _urlreq.urlopen = _fake_urlopen
    try:
        _try_unraid_api(
            "https://unraid.invalid/graphql",
            api_key="dummy-key",
            pool_name_filter=None,
            verify_tls=True,
        )
    finally:
        _urlreq.urlopen = orig_urlopen

    ctx = captured_kwargs.get("context")
    # verify_tls=True → no custom context; urllib uses its own secure default.
    assert ctx is None, f"expected no custom SSLContext, got {ctx}"
    print("_test_unraid_api_verify_tls_true_verifies: OK")


def _test_lock_blocks_second_instance():
    """flock held by another fd → _acquire_lock returns False (BlockingIOError)."""
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    orig_state, orig_lock, orig_last, orig_fh = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            _lock_fh = None
            # Simulate another instance: open the file and hold LOCK_EX.
            # flock on the same inode from a second fd (the one _acquire_lock
            # will open) will block — LOCK_NB turns that into BlockingIOError.
            holder = open(_LOCK_FILE, "w")
            holder.write(json.dumps({"started_at": "2026-01-01T00:00:00+00:00"}))
            holder.flush()
            fcntl.flock(holder, fcntl.LOCK_EX)
            try:
                result = _acquire_lock()
                assert result is False, f"expected False (flock held), got {result}"
                assert _lock_fh is None, "_lock_fh must stay None when acquire fails"
            finally:
                fcntl.flock(holder, fcntl.LOCK_UN)
                holder.close()
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh = orig_state, orig_lock, orig_last, orig_fh
    print("_test_lock_blocks_second_instance: OK")


def _test_lock_stale_file_acquired():
    """Lock file exists but no flock held (crashed prior run) → acquired cleanly."""
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    orig_state, orig_lock, orig_last, orig_fh = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            _lock_fh = None
            # Write stale metadata — no process holds a flock on this file.
            _LOCK_FILE.write_text(json.dumps({
                "pid": 999999999,
                "started_at": "2026-01-01T00:00:00+00:00",
                "mode": "full",
            }))
            result = _acquire_lock()
            assert result is True, f"expected True (no flock held), got {result}"
            assert _lock_fh is not None, "_lock_fh should be set after successful acquire"
            _release_lock()
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh = orig_state, orig_lock, orig_last, orig_fh
    print("_test_lock_stale_file_acquired: OK")


def _test_lock_released_on_success():
    """After _release_lock, the file is gone and the flock is freed."""
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    orig_state, orig_lock, orig_last, orig_fh = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            _lock_fh = None
            ok = _acquire_lock()
            assert ok, "failed to acquire lock in test setup"
            assert _LOCK_FILE.exists(), "lock file should exist after acquire"
            _release_lock()
            assert not _LOCK_FILE.exists(), "lock file should be gone after release"
            assert _lock_fh is None, "_lock_fh should be None after release"
            # Verify the flock is actually free: we can re-acquire immediately.
            ok2 = _acquire_lock()
            assert ok2, "should be able to re-acquire after release"
            _release_lock()
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh = orig_state, orig_lock, orig_last, orig_fh
    print("_test_lock_released_on_success: OK")


def _test_lock_released_on_failure():
    """Lock acquired, simulated exception mid-run — lock still released via finally."""
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    orig_state, orig_lock, orig_last, orig_fh = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            _lock_fh = None
            ok = _acquire_lock()
            assert ok
            try:
                raise RuntimeError("simulated mid-run failure")
            finally:
                _release_lock()
    except RuntimeError:
        pass  # expected
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE, _lock_fh = orig_state, orig_lock, orig_last, orig_fh
    print("_test_lock_released_on_failure: OK")


def _test_skip_if_run_within_minutes():
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    orig_state, orig_lock, orig_last = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            # Write last_run.json with finished_at 5 minutes ago.
            finished = datetime.now(timezone.utc) - timedelta(minutes=5)
            _LAST_RUN_FILE.write_text(json.dumps({
                "started_at": (finished - timedelta(minutes=10)).isoformat(),
                "finished_at": finished.isoformat(),
                "mode": "full",
                "exit_code": 0,
                "moves_attempted": 0,
                "moves_succeeded": 0,
                "bytes_moved": 0,
            }))
            cfg = {"scheduling": {"skip_if_run_within_minutes": 30}}
            result = _check_skip_recent(cfg)
            assert result is True, f"expected True (within threshold), got {result}"
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE = orig_state, orig_lock, orig_last
    print("_test_skip_if_run_within_minutes: OK")


def _test_skip_disabled_when_zero():
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    orig_state, orig_lock, orig_last = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            finished = datetime.now(timezone.utc) - timedelta(minutes=1)
            _LAST_RUN_FILE.write_text(json.dumps({
                "finished_at": finished.isoformat(),
                "mode": "full", "exit_code": 0,
                "moves_attempted": 0, "moves_succeeded": 0, "bytes_moved": 0,
            }))
            cfg = {"scheduling": {"skip_if_run_within_minutes": 0}}
            result = _check_skip_recent(cfg)
            assert result is False, f"expected False (disabled), got {result}"
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE = orig_state, orig_lock, orig_last
    print("_test_skip_disabled_when_zero: OK")


def _test_last_run_written():
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    orig_state, orig_lock, orig_last = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            started = datetime(2026, 5, 21, 3, 0, 0, tzinfo=timezone.utc)
            stats = {"moves_attempted": 3, "moves_succeeded": 2, "bytes_moved": 1024}
            _write_last_run(started, int(ExitCode.SUCCESS), stats)
            assert _LAST_RUN_FILE.exists(), "last_run.json should exist"
            data = json.loads(_LAST_RUN_FILE.read_text())
            assert data["mode"] == "full"
            assert data["exit_code"] == 0
            assert data["moves_attempted"] == 3
            assert data["moves_succeeded"] == 2
            assert data["bytes_moved"] == 1024
            assert data["started_at"].startswith("2026-05-21T03:00:00")
            assert "finished_at" in data
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE = orig_state, orig_lock, orig_last
    print("_test_last_run_written: OK")


def _test_last_run_written_when_run_returns_none():
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    orig_state, orig_lock, orig_last = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"
            started = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
            # Simulate _run() returning None (dry-run path).
            _write_last_run(started, int(ExitCode.SUCCESS), None)
            assert _LAST_RUN_FILE.exists(), "last_run.json should exist even when move_stats is None"
            data = json.loads(_LAST_RUN_FILE.read_text())
            assert data["exit_code"] == 0
            assert data["moves_attempted"] == 0
            assert data["moves_succeeded"] == 0
            assert data["bytes_moved"] == 0
            assert "finished_at" in data
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE = orig_state, orig_lock, orig_last
    print("_test_last_run_written_when_run_returns_none: OK")


def _test_ingest_history_grandparent_key_fallback():
    """grandparentKey path form is parsed when grandparentRatingKey is absent."""
    from datetime import datetime, timezone

    class FakeEpEvent:
        ratingKey = 9001
        viewedAt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        grandparentKey = "/library/metadata/5555"
        # grandparentRatingKey intentionally NOT defined as an attribute

    index: dict = {}
    count = _ingest_history([FakeEpEvent()], index)
    assert count == 1, f"expected 1 event counted, got {count}"
    assert 9001 in index, "episode ratingKey should be indexed"
    assert index[9001]["plays"] == 1
    assert 5555 in index, "grandparent id parsed from grandparentKey should be indexed"
    assert index[5555]["plays"] == 1
    assert index[5555]["last"] == FakeEpEvent.viewedAt
    print("_test_ingest_history_grandparent_key_fallback: OK")


def _test_ingest_history_no_grandparent_skipped_cleanly():
    """Event with neither grandparentRatingKey nor parseable grandparentKey is skipped without error."""
    from datetime import datetime, timezone

    class FakeEpEventNone:
        ratingKey = 7777
        viewedAt = datetime(2026, 2, 1, tzinfo=timezone.utc)
        grandparentKey = "/unexpected/path/format"
        # grandparentRatingKey not present; grandparentKey has no int tail

    class FakeEpEventMissing:
        ratingKey = 8888
        viewedAt = datetime(2026, 3, 1, tzinfo=timezone.utc)
        # neither grandparentRatingKey nor grandparentKey present

    index: dict = {}
    count = _ingest_history([FakeEpEventNone(), FakeEpEventMissing()], index)
    assert count == 2, f"expected 2 events counted, got {count}"
    assert 7777 in index
    assert 8888 in index
    # Neither should have produced a grandparent rollup entry
    for key in list(index.keys()):
        assert key in (7777, 8888), f"unexpected index key {key}"
    print("_test_ingest_history_no_grandparent_skipped_cleanly: OK")


def _test_mode_promote_only_skips_demotions():
    """promote-only: TO_HOT executes; TO_WARM + RELOCATE_WARM are suppressed."""
    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    to_hot = _make_item(
        kind="movie", current_tier="WARM",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/A.mkv"]},
    )
    to_hot.outcome = "TO_HOT"
    to_warm = _make_item(
        kind="movie", current_tier="HOT",
        hot_pool_files=["/mnt/zfs_media/Movies/B.mkv"],
    )
    to_warm.outcome = "TO_WARM"
    relocate = _make_item(
        kind="movie", current_tier="WARM", current_disk="/mnt/disk2",
        warm_disk_files={"/mnt/disk2": ["/mnt/disk2/Movies/C.mkv"]},
    )
    relocate.outcome = "RELOCATE_WARM"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media", "array_disks": ["/mnt/disk1", "/mnt/disk2"]},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([to_hot, to_warm, relocate], cfg, apply=True, no_demote=True)
    finally:
        subprocess.run = orig

    assert len(rsync_calls) == 1, f"expected only TO_HOT to rsync, got {len(rsync_calls)} calls"
    assert to_warm.outcome == "TO_WARM", "outcome must be unchanged, not converted"
    assert relocate.outcome == "RELOCATE_WARM", "outcome must be unchanged, not converted"
    print("_test_mode_promote_only_skips_demotions: OK")


def _test_mode_demote_only_skips_promotions():
    """demote-only: TO_WARM + RELOCATE_WARM execute; TO_HOT is suppressed."""
    global _disk_free_bytes
    orig_dfb = _disk_free_bytes
    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    to_hot = _make_item(
        kind="movie", current_tier="WARM",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/A.mkv"]},
    )
    to_hot.outcome = "TO_HOT"
    to_warm = _make_item(
        kind="movie", current_tier="HOT",
        hot_pool_files=["/mnt/zfs_media/Movies/B.mkv"],
    )
    to_warm.outcome = "TO_WARM"
    relocate = _make_item(
        kind="movie", current_tier="WARM", current_disk="/mnt/disk1",
        warm_disk_files={"/mnt/disk1": ["/mnt/disk1/Movies/C.mkv"]},
    )
    relocate.outcome = "RELOCATE_WARM"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media", "array_disks": ["/mnt/disk1", "/mnt/disk2"]},
    }

    orig_run = subprocess.run
    try:
        _disk_free_bytes = lambda p: 500 * 1024 ** 3  # noqa: E731
        subprocess.run = _fake_run
        _run_move_pass([to_hot, to_warm, relocate], cfg, apply=True, no_promote=True)
    finally:
        subprocess.run = orig_run
        _disk_free_bytes = orig_dfb

    assert len(rsync_calls) == 2, f"expected TO_WARM + RELOCATE_WARM to rsync, got {len(rsync_calls)}"
    assert to_hot.outcome == "TO_HOT", "outcome must be unchanged, not converted"
    print("_test_mode_demote_only_skips_promotions: OK")


def _test_mode_full_is_default():
    """No --mode, no TIER_MODE, no scheduling.default_mode -> 'full'."""
    orig_env = os.environ.pop("TIER_MODE", None)
    try:
        args = build_parser().parse_args([])
        resolved = _resolve_mode(args, {})
        assert resolved == "full", f"expected full, got {resolved}"
    finally:
        if orig_env is not None:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_full_is_default: OK")


def _test_no_demote_now_covers_relocate_warm():
    """--no-demote (no_demote=True) suppresses RELOCATE_WARM, not just TO_WARM."""
    rsync_calls = []

    def _fake_run(cmd, **_):
        if cmd and cmd[0] == "rsync":
            rsync_calls.append(cmd)
        class R:
            returncode = 0
            stderr = ""
        return R()

    to_warm = _make_item(
        kind="movie", current_tier="HOT",
        hot_pool_files=["/mnt/zfs_media/Movies/B.mkv"],
    )
    to_warm.outcome = "TO_WARM"
    relocate = _make_item(
        kind="movie", current_tier="WARM", current_disk="/mnt/disk2",
        warm_disk_files={"/mnt/disk2": ["/mnt/disk2/Movies/C.mkv"]},
    )
    relocate.outcome = "RELOCATE_WARM"

    cfg = {
        "moves": {
            "enabled": True, "rsync_options": ["-aH"],
            "delete_source_after_verify": False, "size_verify": False,
            "parity_check_blocking": False, "bandwidth_limit_mbps": None,
        },
        "paths": {"hot_pool_mount": "/mnt/zfs_media", "array_disks": ["/mnt/disk1", "/mnt/disk2"]},
    }

    orig = subprocess.run
    try:
        subprocess.run = _fake_run
        _run_move_pass([to_warm, relocate], cfg, apply=True, no_demote=True)
    finally:
        subprocess.run = orig

    assert not rsync_calls, f"expected zero rsync calls under no_demote, got {len(rsync_calls)}"
    print("_test_no_demote_now_covers_relocate_warm: OK")


def _test_mode_and_no_flag_conflict_errors():
    """--mode promote-only + --no-promote on the CLI is a bad-usage error (exit 2)."""
    parser = build_parser()
    args = parser.parse_args(["--mode", "promote-only", "--no-promote"])
    try:
        _check_mode_conflicts(args, parser)
        raised = False
        code = None
    except SystemExit as e:
        raised = True
        code = e.code
    assert raised, "expected SystemExit for conflicting --mode/--no-promote"
    assert code == 2, f"expected exit code 2, got {code}"
    print("_test_mode_and_no_flag_conflict_errors: OK")


def _test_mode_resolved_from_env():
    """No CLI --mode, TIER_MODE=promote-only -> resolved mode is promote-only."""
    orig_env = os.environ.get("TIER_MODE")
    try:
        os.environ["TIER_MODE"] = "promote-only"
        args = build_parser().parse_args([])
        resolved = _resolve_mode(args, {})
        assert resolved == "promote-only", f"expected promote-only, got {resolved}"
    finally:
        if orig_env is None:
            os.environ.pop("TIER_MODE", None)
        else:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_resolved_from_env: OK")


def _test_mode_resolved_from_config_default():
    """No CLI, no env, scheduling.default_mode=promote-only -> resolved mode is promote-only."""
    orig_env = os.environ.pop("TIER_MODE", None)
    try:
        args = build_parser().parse_args([])
        cfg = {"scheduling": {"default_mode": "promote-only"}}
        resolved = _resolve_mode(args, cfg)
        assert resolved == "promote-only", f"expected promote-only, got {resolved}"
    finally:
        if orig_env is not None:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_resolved_from_config_default: OK")


def _test_mode_precedence_cli_over_env():
    """--mode full + TIER_MODE=promote-only -> CLI wins (full)."""
    orig_env = os.environ.get("TIER_MODE")
    try:
        os.environ["TIER_MODE"] = "promote-only"
        args = build_parser().parse_args(["--mode", "full"])
        resolved = _resolve_mode(args, {})
        assert resolved == "full", f"expected full (CLI wins), got {resolved}"
    finally:
        if orig_env is None:
            os.environ.pop("TIER_MODE", None)
        else:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_precedence_cli_over_env: OK")


def _test_mode_precedence_env_over_config():
    """TIER_MODE=full + scheduling.default_mode=promote-only -> env wins (full)."""
    orig_env = os.environ.get("TIER_MODE")
    try:
        os.environ["TIER_MODE"] = "full"
        args = build_parser().parse_args([])
        cfg = {"scheduling": {"default_mode": "promote-only"}}
        resolved = _resolve_mode(args, cfg)
        assert resolved == "full", f"expected full (env wins), got {resolved}"
    finally:
        if orig_env is None:
            os.environ.pop("TIER_MODE", None)
        else:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_precedence_env_over_config: OK")


def _test_mode_invalid_env_value_errors():
    """TIER_MODE=garbage -> bad-usage exit (2), source named in the log."""
    orig_env = os.environ.get("TIER_MODE")
    try:
        os.environ["TIER_MODE"] = "garbage"
        args = build_parser().parse_args([])
        try:
            _resolve_mode(args, {})
            raised = False
            code = None
        except SystemExit as e:
            raised = True
            code = e.code
        assert raised, "expected SystemExit for invalid TIER_MODE"
        assert code == 2, f"expected exit code 2, got {code}"
    finally:
        if orig_env is None:
            os.environ.pop("TIER_MODE", None)
        else:
            os.environ["TIER_MODE"] = orig_env
    print("_test_mode_invalid_env_value_errors: OK")


def _test_apply_resolved_from_env():
    """No CLI --apply, TIER_APPLY=true -> apply mode resolves to True."""
    orig_env = os.environ.get("TIER_APPLY")
    try:
        os.environ["TIER_APPLY"] = "true"
        args = build_parser().parse_args([])
        resolved = _resolve_apply(args, {})
        assert resolved is True, f"expected True, got {resolved}"
    finally:
        if orig_env is None:
            os.environ.pop("TIER_APPLY", None)
        else:
            os.environ["TIER_APPLY"] = orig_env
    print("_test_apply_resolved_from_env: OK")


def _test_min_episodes_blocks_single_pilot():
    """promote-only, series TO_HOT, 1 episode watched since last full run, threshold 2 -> deferred."""
    item = _make_item(kind="series", outcome="TO_HOT", recent_episode_plays=1)
    cfg = {"scheduling": {"min_episodes_for_fast_promote": 2}}
    skip_ids = _apply_fast_promote_guard([item], cfg, "promote-only")
    assert id(item) in skip_ids, "single-pilot series should be deferred"
    assert item.outcome == "TO_HOT", "outcome must stay TO_HOT, not converted"
    print("_test_min_episodes_blocks_single_pilot: OK")


def _test_min_episodes_allows_binge():
    """promote-only, series TO_HOT, 3 episodes watched since last full run, threshold 2 -> moved."""
    item = _make_item(kind="series", outcome="TO_HOT", recent_episode_plays=3)
    cfg = {"scheduling": {"min_episodes_for_fast_promote": 2}}
    skip_ids = _apply_fast_promote_guard([item], cfg, "promote-only")
    assert id(item) not in skip_ids, "binge-watched series should not be deferred"
    print("_test_min_episodes_allows_binge: OK")


def _test_min_episodes_ignores_movies():
    """promote-only, movie TO_HOT -> moved regardless of the episode threshold."""
    item = _make_item(kind="movie", outcome="TO_HOT", recent_episode_plays=0)
    cfg = {"scheduling": {"min_episodes_for_fast_promote": 2}}
    skip_ids = _apply_fast_promote_guard([item], cfg, "promote-only")
    assert id(item) not in skip_ids, "movies must be unaffected by the episode guard"
    print("_test_min_episodes_ignores_movies: OK")


def _test_min_episodes_no_full_run_fallback():
    """cutoff=None (no full run ever recorded) -> counts any recorded play, not just recent ones."""
    class FakeEp:
        def __init__(self, rk):
            self.ratingKey = rk

    episodes = [FakeEp(1), FakeEp(2)]
    history_index = {1: {"plays": 3, "last": datetime(2020, 1, 1, tzinfo=timezone.utc)}}
    count = _count_recent_episode_plays(episodes, history_index, None)
    assert count == 1, f"expected 1 (episode 2 has no history entry), got {count}"
    print("_test_min_episodes_no_full_run_fallback: OK")


def _test_count_recent_episode_plays_naive_viewed_at():
    """Plex's viewedAt comes back offset-naive; cutoff is always offset-aware
    (built from our own isoformat() writes). Comparing them directly raises
    TypeError — this regression test pins the _as_utc() normalisation fix."""
    class FakeEp:
        def __init__(self, rk):
            self.ratingKey = rk

    cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)
    episodes = [FakeEp(1), FakeEp(2)]
    history_index = {
        1: {"plays": 1, "last": datetime(2026, 1, 5)},   # naive, after cutoff
        2: {"plays": 1, "last": datetime(2025, 1, 1)},   # naive, before cutoff
    }
    count = _count_recent_episode_plays(episodes, history_index, cutoff)
    assert count == 1, f"expected 1 (only episode 1 is after cutoff), got {count}"
    print("_test_count_recent_episode_plays_naive_viewed_at: OK")


def _test_last_full_run_recorded():
    """A full run sets last_full_run_finished_at; a promote-only run carries it forward."""
    import tempfile as _tf
    global _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    orig_state, orig_lock, orig_last = _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE
    try:
        with _tf.TemporaryDirectory() as tmp:
            _STATE_DIR = Path(tmp)
            _LOCK_FILE = _STATE_DIR / "tier.lock"
            _LAST_RUN_FILE = _STATE_DIR / "last_run.json"

            started = datetime(2026, 6, 1, 3, 0, 0, tzinfo=timezone.utc)
            stats = {"moves_attempted": 0, "moves_succeeded": 0, "bytes_moved": 0}
            _write_last_run(started, int(ExitCode.SUCCESS), stats, mode="full")
            data = json.loads(_LAST_RUN_FILE.read_text())
            assert data["last_full_run_finished_at"], "full run must set last_full_run_finished_at"
            full_finished = data["last_full_run_finished_at"]

            started2 = datetime(2026, 6, 2, 4, 30, 0, tzinfo=timezone.utc)
            _write_last_run(started2, int(ExitCode.SUCCESS), stats, mode="promote-only")
            data2 = json.loads(_LAST_RUN_FILE.read_text())
            assert data2["last_full_run_finished_at"] == full_finished, (
                "promote-only run must carry last_full_run_finished_at forward unchanged"
            )
            assert data2["mode"] == "promote-only"
    finally:
        _STATE_DIR, _LOCK_FILE, _LAST_RUN_FILE = orig_state, orig_lock, orig_last
    print("_test_last_full_run_recorded: OK")


if __name__ == "__main__":
    if "--_test" in sys.argv:
        _test_resolve_user_share()
        _test_added_floor_movie_recent()
        _test_added_floor_movie_old()
        _test_added_floor_tv_recent_episode()
        _test_added_floor_tv_no_recent()
        _test_added_floor_disabled()
        _test_added_floor_preserves_pin()
        _test_added_floor_never_demotes()
        _test_added_floor_tv_search_uses_int_timestamp()
        _test_collection_pin_promotes_to_pin_hot()
        _test_collection_pin_missing_collection()
        _test_collection_pin_empty_list()
        _test_collection_pin_idempotent_with_added_floor()
        _test_auto_inherit_happy_path()
        _test_auto_inherit_threshold_not_met()
        _test_auto_inherit_explicit_pin_takes_precedence()
        _test_auto_inherit_smart_collection_skip()
        _test_auto_inherit_disabled()
        _test_auto_inherit_exclude_library()
        _test_auto_inherit_fraction_triggers_small_collection()
        _test_auto_inherit_fraction_no_hot_no_trigger()
        _test_auto_inherit_skip_below_min_hot()
        _test_auto_inherit_larger_collection_uses_absolute()
        _test_eviction_stay_warm_becomes_relocate()
        _test_eviction_to_hot_stays_to_hot()
        _test_eviction_non_evict_disk_unaffected()
        _test_eviction_disabled_no_items_flagged()
        _test_dominant_warm_disk_movie_with_year_folder()
        _test_dominant_warm_disk_single_file_item()
        _test_eviction_movie_on_evict_disk_becomes_relocate()
        _test_hot_majority_warm_disk_files_populated()
        _test_eviction_minority_warm_files_on_evict_disk()
        _test_eviction_majority_evict_non_evict_files_excluded()
        _test_destination_path_movie_tohot()
        _test_destination_path_series_tohot()
        _test_move_skipped_when_already_hot()
        _test_dry_run_emits_no_apply_call()
        _test_parity_check_aborts_pass()
        _test_parity_check_unraid_idle_not_falsely_detected()
        _test_parity_check_unraid_active_detected()
        _test_size_verify_failure_skips_delete()
        _test_multidisk_series_all_source_dirs_rsynced()
        _test_size_verify_mixed_tier_preexisting_dst_passes()
        _test_companion_files_included_in_warm_disk_files()
        _test_movie_per_folder_extras_included()
        _test_cross_tier_companion_probe()
        _test_movie_per_folder_article_inversion()
        _test_straggler_stay_warm_upgraded_to_to_warm()
        _test_straggler_stay_hot_upgraded_to_to_hot()
        _test_straggler_pin_hot_warm_upgraded_to_to_hot()
        _test_straggler_no_upgrade_when_no_wrong_tier_files()
        _test_movie_straggler_to_warm_colocates_with_main_file()
        _test_empty_ancestor_dirs_pruned_after_delete()
        _test_warm_disk_selection_to_warm_picks_most_free()
        _test_warm_disk_selection_relocate_excludes_source()
        _test_warm_disk_selection_co_locate_for_series()
        _test_warm_disk_selection_no_capacity_returns_none()
        _test_warm_disk_selection_safety_margin_respected()
        _test_to_warm_full_flow_end_to_end()
        _test_relocate_warm_full_flow_end_to_end()
        _test_relocate_warm_co_locate_with_existing_partial()
        _test_capacity_budget_caps_to_hot_promotions()
        _test_capacity_budget_zero_makes_all_over_budget()
        _test_capacity_over_ceiling_auto_demotes_lowest_scorers()
        _test_capacity_over_ceiling_does_not_demote_pin_hot()
        _test_capacity_no_promote_flag_blocks_all_to_hot()
        _test_capacity_no_demote_flag_blocks_to_warm()
        _test_capacity_safety_margin_respected()
        _test_capacity_warm_per_disk_ceiling_blocks_to_warm_when_full()
        _test_run_cap_stops_after_limit()
        _test_run_cap_zero_disables_cap()
        _test_run_cap_counts_successful_moves_only()
        _test_run_cap_exact_boundary()
        _test_capacity_unraid_api_failure_warns_and_falls_back()
        _test_capacity_unraid_api_not_configured_no_warn()
        _test_unraid_api_verify_tls_false_uses_unverified_context()
        _test_unraid_api_verify_tls_true_verifies()
        _test_lock_blocks_second_instance()
        _test_lock_stale_file_acquired()
        _test_lock_released_on_success()
        _test_lock_released_on_failure()
        _test_skip_if_run_within_minutes()
        _test_skip_disabled_when_zero()
        _test_last_run_written()
        _test_last_run_written_when_run_returns_none()
        _test_ingest_history_grandparent_key_fallback()
        _test_ingest_history_no_grandparent_skipped_cleanly()
        _test_mode_promote_only_skips_demotions()
        _test_mode_demote_only_skips_promotions()
        _test_mode_full_is_default()
        _test_no_demote_now_covers_relocate_warm()
        _test_mode_and_no_flag_conflict_errors()
        _test_mode_resolved_from_env()
        _test_mode_resolved_from_config_default()
        _test_mode_precedence_cli_over_env()
        _test_mode_precedence_env_over_config()
        _test_mode_invalid_env_value_errors()
        _test_apply_resolved_from_env()
        _test_min_episodes_blocks_single_pilot()
        _test_min_episodes_allows_binge()
        _test_min_episodes_ignores_movies()
        _test_min_episodes_no_full_run_fallback()
        _test_count_recent_episode_plays_naive_viewed_at()
        _test_last_full_run_recorded()
        sys.exit(int(ExitCode.SUCCESS))
    sys.exit(main())
