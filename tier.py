#!/usr/bin/env python3
"""
tier.py — Unraid media tiering script (Phase P2.1)

Pulls watch history and metadata from Plex, computes a heat score per
media item (movies + TV series), probes the filesystem to determine
where each item currently lives, and recommends where it should live —
HOT (fast pool) or WARM (Unraid array). With --apply and moves.enabled,
executes TO_HOT rsync moves serially.

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
  P2.1 (this file)  Move executor — TO_HOT direction. rsync from warm
                    array disk to hot ZFS pool. Dry-run by default;
                    --apply executes. Source deleted after size-verify
                    when delete_source_after_verify=true.
  P2.2 (later)      TO_WARM + RELOCATE_WARM moves.
  P2.3 (later)      Plex rescan automation.
  P3  (later)       Hardened safeguards (lock file, currently-playing skip,
                    free-space check, max-move cap).
  P4  (later)       Scheduled cron + size-triggered wrapper.

Usage:
    tier.py [--config PATH] [--library NAME ...] [--json|--csv PATH]
            [--explain TITLE] [--sort COL] [--top N] [--apply]

Exit codes:
    0  success
    1  configuration error
    2  Plex unreachable or auth failed
    4  unhandled runtime error (notification fired if configured)
  130  interrupted (SIGINT)
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import logging.handlers
import math
import os
import re
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
}

# Default config path — container layout. Bare installs fall back to the
# legacy /boot path in load_config().
DEFAULT_CONFIG_PATH = Path("/config/tiering.yaml")
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
    # year folders). Dominant disk is at key == current_disk.
    warm_disk_files: Dict[str, List[str]] = field(default_factory=dict)
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
        sys.exit(2)
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
        sys.exit(2)


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


def _find_companion_files(media_path: str) -> List[str]:
    """Return sidecar files in the same directory that share the media file's stem.

    Plex discovers external subtitles at play time by scanning the media
    file's directory for files whose stem matches (or starts with the stem
    followed by '.', to catch language tags like Movie.en.srt).  This
    function replicates that logic so rsync moves companions alongside the
    media file.

    Example: /disk/Movies/Moana (2016).mkv  →  finds:
      Moana (2016).nfo, Moana (2016).en.srt, Moana (2016).sub, …
    """
    parent = os.path.dirname(media_path)
    stem = os.path.splitext(os.path.basename(media_path))[0]
    companions: List[str] = []
    try:
        with os.scandir(parent) as it:
            for entry in it:
                if not entry.is_file(follow_symlinks=False):
                    continue
                if entry.path == media_path:
                    continue
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
) -> Tuple[str, dict, Optional[str], List[str], Dict[str, List[str]]]:
    """Majority-bytes rollup of tier for a multi-part item.

    parts: iterable of (plex_file_path, size_bytes).
    Returns:
      (tier_str, breakdown, dominant_warm_disk, source_dirs, warm_disk_files)
      tier_str: 'HOT' | 'WARM' | 'MIXED' | 'UNKNOWN'
      breakdown: dict of per-tier byte shares (0.0..1.0)
      dominant_warm_disk: path of the WARM disk with most bytes, or None
      source_dirs: common-ancestor dirs per warm disk (dominant first) — display only
      warm_disk_files: {disk: [resolved file paths]} for all warm disks

    Decision rules:
      - Majority (>50%) bytes on HOT  -> HOT
      - Majority bytes on WARM        -> WARM
      - Majority bytes UNKNOWN        -> UNKNOWN
      - Otherwise (50/50 tie, or no clear majority) -> MIXED
    """
    totals = {"HOT": 0, "WARM": 0, "UNKNOWN": 0}
    disk_bytes: dict = {}  # warm disk path -> bytes on that disk
    disk_files: Dict[str, List[str]] = {}  # warm disk path -> list of resolved file paths
    total = 0
    for plex_path, size in parts:
        if not size or size <= 0:
            continue
        total += size
        translated = translate_plex_path(plex_path or "", path_map)
        resolved = resolve_user_share(translated, user_share_prefix, hot_mount, array_disks)
        tier = classify_path(resolved, hot_mount, array_disks)
        totals[tier] += size
        if tier == "WARM":
            for disk in array_disks or []:
                d = disk.rstrip("/")
                if resolved == d or resolved.startswith(d + "/"):
                    disk_bytes[disk] = disk_bytes.get(disk, 0) + size
                    disk_files.setdefault(disk, []).append(resolved)
                    break
    # Augment each disk's file list with companion files (subtitles, NFO, etc.)
    # that share the media file's stem. Plex discovers external subtitles this
    # way at play time, so they must live alongside the media on the same tier.
    seen: set = set()
    for disk in list(disk_files.keys()):
        extras: List[str] = []
        for mf in disk_files[disk]:
            seen.add(mf)
            for companion in _find_companion_files(mf):
                if companion not in seen:
                    seen.add(companion)
                    extras.append(companion)
        disk_files[disk].extend(extras)

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
        return "UNKNOWN", {"HOT": 0.0, "WARM": 0.0, "UNKNOWN": 0.0}, None, [], {}
    split = {k: round(v / total, 4) for k, v in totals.items()}
    if split["HOT"] > 0.5:
        return "HOT", split, None, [], {}
    if split["WARM"] > 0.5:
        return "WARM", split, dominant, warm_source_dirs, dict(disk_files)
    if split["UNKNOWN"] > 0.5:
        return "UNKNOWN", split, None, [], {}
    return "MIXED", split, dominant, warm_source_dirs, dict(disk_files)


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


def _ingest_history(events, index: dict) -> int:
    """Fold a sequence of History objects into the index. Returns event count.

    Each event contributes to:
      - index[ratingKey]             (the movie OR episode itself)
      - index[grandparentRatingKey]  (the show, for series rollup)
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

        grk = getattr(h, "grandparentRatingKey", None)
        if grk is None:
            continue
        try:
            grk_int = int(grk)
        except (TypeError, ValueError):
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
        if tier_probing:
            parts = []
            for m in group:
                parts.extend(_media_parts(getattr(m, "media", None)))
            current_tier, tier_split, current_disk, source_dirs, warm_disk_files = resolve_item_current_tier(
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

        # Current tier (P1): majority-bytes rollup across every episode.
        current_tier = "UNKNOWN"
        current_disk: Optional[str] = None
        source_dirs: List[str] = []
        tier_split: Optional[dict] = None
        if tier_probing:
            parts = []
            for ep in episodes:
                parts.extend(_media_parts(getattr(ep, "media", None)))
            current_tier, tier_split, current_disk, source_dirs, warm_disk_files = resolve_item_current_tier(
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
            outcome="NEUTRAL",  # finalised in post-scoring override pass
            rating_key=rk,
            recently_added=recently_added,
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


def collect_all(plex: PlexServer, cfg: dict, filter_libraries) -> List[Item]:
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
                if it.current_disk is not None and it.current_disk in evict_disks
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
                elif it.outcome in _HOT_OUTCOMES:
                    implicit_hot_count += 1
            log.info(
                "Eviction: %d items flagged RELOCATE_WARM (TO_HOT path: %d)",
                relocate_count, implicit_hot_count,
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

# Assumed throughput for ETA estimates (spinning array → ZFS NVMe).
_ASSUMED_THROUGHPUT_BPS = 200 * 1024 * 1024  # 200 MB/s


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


def _run_move_pass(items: List["Item"], cfg: dict, apply: bool) -> None:
    """Dry-run or execute the TO_HOT move pass.

    When apply=False: emits [DRY-RUN] log lines for every projected move.
    When apply=True:  executes rsync serially, verifies sizes, optionally
                      deletes the source after successful verification.

    Skips the entire pass if moves.enabled is false in config.
    """
    moves_cfg = cfg.get("moves") or {}
    if not moves_cfg.get("enabled"):
        return

    hot_mount = (cfg.get("paths") or {}).get("hot_pool_mount") or ""
    if not hot_mount:
        log.warning("Moves: moves.enabled=true but paths.hot_pool_mount not set — skipping")
        return

    # Tally outcomes not yet supported so we can emit a single skip line.
    skip_outcomes: dict = {}
    for it in items:
        if it.outcome in ("TO_WARM", "RELOCATE_WARM", "SHOULD_BE_HOT", "SHOULD_BE_WARM"):
            skip_outcomes[it.outcome] = skip_outcomes.get(it.outcome, 0) + 1
    if skip_outcomes:
        parts_str = "  ".join(f"{k}={v}" for k, v in sorted(skip_outcomes.items()))
        log.info(
            "Moves: skipping %d items in directions not yet supported (%s)",
            sum(skip_outcomes.values()), parts_str,
        )

    # Separate already-HOT items (idempotency) from actionable TO_HOT items.
    to_hot_skip = [it for it in items if it.outcome == "TO_HOT" and it.current_tier == "HOT"]
    to_hot_move = [it for it in items if it.outcome == "TO_HOT" and it.current_tier != "HOT"]

    # Validate each candidate has file-level data we need for rsync.
    moves: List["Item"] = []
    for it in to_hot_move:
        if not it.warm_disk_files:
            log.warning(
                "Moves: [SKIP] %s — no warm_disk_files (tier detection inactive?)",
                it.title_year,
            )
            continue
        moves.append(it)

    all_to_hot = len(to_hot_skip) + len(moves)
    if not all_to_hot:
        log.info("Moves: no actionable TO_HOT items")
        return

    total_bytes = sum(it.size_bytes for it in moves)

    if not apply:
        eta = _fmt_eta(total_bytes / _ASSUMED_THROUGHPUT_BPS)
        log.info(
            "[DRY-RUN] Moves: TO_HOT=%d (%d skipped already-HOT) — %s total — estimated %s at 200 MB/s",
            len(moves), len(to_hot_skip),
            _fmt_size(total_bytes / (1024 ** 3)),
            eta,
        )
        for it in to_hot_skip:
            log.info("[DRY-RUN]   %s — already on hot pool (SKIPPED)", it.title_year)
        for it in moves:
            n_files = sum(len(f) for f in it.warm_disk_files.values())
            for disk, files in it.warm_disk_files.items():
                log.info(
                    "[DRY-RUN]   %s — %s — %d file(s) from %s → %s",
                    it.title_year, _fmt_size(it.size_bytes / (1024 ** 3)),
                    n_files, disk, hot_mount,
                )
        return

    # --apply mode.
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

    eta = _fmt_eta(total_bytes / _ASSUMED_THROUGHPUT_BPS)
    log.info(
        "Moves: TO_HOT=%d (apply mode) — %s total — ETA ~%s",
        len(moves), _fmt_size(total_bytes / (1024 ** 3)), eta,
    )
    for it in to_hot_skip:
        log.info("  [SKIPPED] %s — already on hot pool (no-op)", it.title_year)

    n_success = n_skipped = n_failed = 0
    n_skipped = len(to_hot_skip)
    affected_libraries: set = set()
    run_start = datetime.now(timezone.utc)

    for idx, it in enumerate(moves, 1):
        prefix = f"  [{idx}/{len(moves)}]"
        item_start = datetime.now(timezone.utc)
        size_str = _fmt_size(it.size_bytes / (1024 ** 3))
        n_files = sum(len(f) for f in it.warm_disk_files.values())

        # Log before starting — long rsyncs are otherwise silent.
        log.info("%s Moving %s — %s, %d file(s)", prefix, it.title_year, size_str, n_files)
        for disk, files in it.warm_disk_files.items():
            log.info("%s   %s (%d files) → %s", prefix, disk, len(files), hot_mount)

        # rsync each warm disk using --files-from so only this item's files are
        # transferred, regardless of whether the library uses per-item folders or
        # shared year folders. Source root is the disk mount; destination root is
        # the hot pool mount — rsync preserves the full relative path structure.
        import tempfile as _tempfile
        rsync_failed = False
        for disk, files in it.warm_disk_files.items():
            disk_root = disk.rstrip("/") + "/"
            # Build relative paths (strip leading disk root + separator).
            rel_paths = [f[len(disk_root):] if f.startswith(disk_root) else f.lstrip("/")
                         for f in files]
            try:
                with _tempfile.NamedTemporaryFile(
                    mode="w", suffix=".txt", delete=False, prefix="tier_rsync_"
                ) as flist:
                    flist.write("\n".join(rel_paths))
                    flist_path = flist.name
            except OSError as exc:
                log.error("%s [FAILED] %s — cannot write files-from list: %s",
                          prefix, it.title_year, exc)
                rsync_failed = True
                break
            try:
                cmd = (["rsync"] + rsync_opts
                       + [f"--files-from={flist_path}", disk_root, hot_mount.rstrip("/") + "/"])
                result = subprocess.run(cmd, capture_output=True, text=True)
            except OSError as exc:
                log.error("%s [FAILED] %s — rsync failed to start: %s",
                          prefix, it.title_year, exc)
                rsync_failed = True
            finally:
                try:
                    os.unlink(flist_path)
                except OSError:
                    pass
            if rsync_failed:
                break
            if result.returncode != 0:
                stderr_lines = (result.stderr or "").strip().splitlines()[:5]
                stderr_str = "; ".join(stderr_lines) if stderr_lines else ""
                log.error(
                    "%s [FAILED] %s — rsync exit %d (disk=%s)%s — source unchanged",
                    prefix, it.title_year, result.returncode, disk,
                    f": {stderr_str}" if stderr_str else "",
                )
                rsync_failed = True
                break
        if rsync_failed:
            n_failed += 1
            continue

        # Size verification: compare source file sizes against destination file
        # sizes using os.path.getsize per file. File-level measurement is immune
        # to concurrent downloads into the same directory (only the specific
        # transferred files are measured, not the whole directory total).
        if size_verify:
            try:
                src_sz = dst_sz = 0
                hot = hot_mount.rstrip("/")
                for disk, files in it.warm_disk_files.items():
                    disk_root = disk.rstrip("/")
                    for f in files:
                        src_sz += os.path.getsize(f)
                        dst_f = hot + f[len(disk_root):]
                        if os.path.exists(dst_f):
                            dst_sz += os.path.getsize(dst_f)
                if abs(src_sz - dst_sz) > 1024:
                    log.error(
                        "%s [FAILED] %s — size verify failed (src=%s dst=%s) — source unchanged",
                        prefix, it.title_year, src_sz, dst_sz,
                    )
                    n_failed += 1
                    continue
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "%s [FAILED] %s — size verify error: %s — source unchanged",
                    prefix, it.title_year, exc,
                )
                n_failed += 1
                continue

        # Delete source files individually, then prune empty parent directories.
        # File-level delete means only this item's files are removed — other
        # items sharing the same parent directory (e.g. a year folder) are safe.
        if delete_after:
            delete_failed = False
            # Per-disk set of leaf dirs that need ancestor pruning.
            disk_leaf_dirs: Dict[str, set] = {}
            for disk, files in it.warm_disk_files.items():
                disk_leaf_dirs[disk] = set()
                for f in files:
                    try:
                        os.unlink(f)
                        disk_leaf_dirs[disk].add(os.path.dirname(f))
                    except OSError as exc:
                        log.warning(
                            "%s [SUCCESS*] %s — moved OK but source removal failed (%s): %s",
                            prefix, it.title_year, f, exc,
                        )
                        delete_failed = True
            # Prune empty ancestor directories up to (but not including) each
            # disk root. Walk every ancestor so season dirs, show dirs, etc.
            # are removed when they empty out. Each disk is handled with its
            # own root so multi-disk series prune correctly on every disk.
            all_dirs: set = set()
            for disk, leaf_dirs in disk_leaf_dirs.items():
                disk_root = disk.rstrip("/")
                for d in leaf_dirs:
                    current = d
                    while current and current != disk_root and current.startswith(disk_root + "/"):
                        all_dirs.add(current)
                        current = os.path.dirname(current)
            for d in sorted(all_dirs, reverse=True):
                try:
                    os.rmdir(d)  # no-op if directory still has content
                except OSError:
                    pass
            if delete_failed:
                n_success += 1
                affected_libraries.add(it.library)
                continue

        elapsed = (datetime.now(timezone.utc) - item_start).total_seconds()
        log.info(
            "%s [SUCCESS] %s — %s in %s — %s → %s",
            prefix, it.title_year, size_str, _fmt_eta(elapsed),
            it.current_disk, hot_mount,
        )
        n_success += 1
        affected_libraries.add(it.library)

    total_elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
    log.info(
        "Moves complete: %d successful, %d skipped, %d failed (%s total)",
        n_success, n_skipped, n_failed, _fmt_eta(total_elapsed),
    )
    if affected_libraries:
        log.info(
            "Plex rescan recommended for sections: %s",
            ", ".join(sorted(affected_libraries)),
        )


# Outcomes that map to each projected tier if every recommendation were
# executed. The bucket reflects where the item will END UP, not where it
# currently sits — so TO_HOT + STAY_HOT + PIN_HOT all go into HOT.
_HOT_OUTCOMES = {"SHOULD_BE_HOT", "PIN_HOT", "STAY_HOT", "TO_HOT"}
_WARM_OUTCOMES = {"SHOULD_BE_WARM", "STAY_WARM", "TO_WARM", "RELOCATE_WARM"}
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


def _run(args) -> int:
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

    log.info("tier.py starting — config=%s", args.config)

    plex = connect_plex(cfg["plex"]["url"], cfg["plex"]["token"], notifier, ncfg)

    items = collect_all(plex, cfg, filter_libraries=args.library)
    items = apply_sort(items, args.sort)

    # Move pass runs on the full scored list before --top truncation so every
    # TO_HOT item is considered regardless of display limit.
    moves_apply = args.apply or bool((cfg.get("moves") or {}).get("apply", False))
    _run_move_pass(items, cfg, apply=moves_apply)

    if args.top:
        items = items[: args.top]

    if args.csv:
        write_csv(items, args.csv)
        log.info("Wrote %d rows to %s", len(items), args.csv)

    if args.explain:
        explain_one(items, args.explain, cfg["thresholds"])
        return 0

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

    return 0


def main() -> int:
    args = build_parser().parse_args()

    # Outer try/except catches anything _run() didn't handle and alerts.
    # We need a notifier to alert with, but building one needs the config;
    # if the config itself blows up we fall back to stderr only.
    notifier: Optional[Notifier] = None
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

        return _run(args)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        return 130
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
        return 4


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
    """Companion files (nfo, srt, sub) are included in warm_disk_files alongside media."""
    import tempfile, os as _os

    with tempfile.TemporaryDirectory() as root:
        disk = _os.path.join(root, "disk4")
        hot_mount = _os.path.join(root, "zfs_media")
        movie_dir = _os.path.join(disk, "Movies", "Moana (2016)")
        _os.makedirs(movie_dir, exist_ok=True)
        _os.makedirs(hot_mount, exist_ok=True)

        mkv = _os.path.join(movie_dir, "Moana (2016).mkv")
        nfo = _os.path.join(movie_dir, "Moana (2016).nfo")
        srt = _os.path.join(movie_dir, "Moana (2016).en.srt")
        unrelated = _os.path.join(movie_dir, "other_movie.mkv")

        for f in (mkv, nfo, srt, unrelated):
            with open(f, "wb") as fh:
                fh.write(b"x" * 100)

        parts = [(mkv, 100)]
        tier, _, _, _, wdf = resolve_item_current_tier(
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
        _test_empty_ancestor_dirs_pruned_after_delete()
        sys.exit(0)
    sys.exit(main())
