#!/usr/bin/env python3
"""
tier.py — Unraid media tiering script (Phase P1, read-only)

Pulls watch history and metadata from Plex, computes a heat score per
media item (movies + TV series), probes the filesystem to determine
where each item currently lives, and prints a table showing the
recommended tier placement: HOT (fast pool), WARM (Unraid array), or
STAY.

Phase status:
  P0  (done)        Read-only analysis from Plex catalog + watch history.
  P0.1 (done)       Pinning (library + title), recency floor, projected-
                    tier footer.
  P1  (this file)   Filesystem probing to detect current tier. Auto-
                    detects array disks, translates Plex-side paths via
                    plex_path_map, rolls multi-part items up by bytes.
  P2  (later)       Adds --apply with rsync moves + Plex rescan.
  P3  (later)       Hardened safeguards (lock file, currently-playing skip,
                    free-space check, max-move cap).
  P4  (later)       Scheduled cron + size-triggered wrapper.

Usage:
    tier.py [--config PATH] [--library NAME ...] [--json|--csv PATH]
            [--explain TITLE] [--sort COL] [--top N]

Exit codes:
    0  success
    1  configuration error
    2  Plex unreachable or auth failed
    3  --apply used in P0 (not yet implemented)
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

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
    # --- decision ---
    outcome: str = "NEUTRAL"       # See decide_outcome() for P0 values
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


def resolve_item_current_tier(
    parts: Iterable[Tuple[str, int]],
    path_map,
    hot_mount: str,
    array_disks: List[str],
    user_share_prefix: str = "",
) -> Tuple[str, dict]:
    """Majority-bytes rollup of tier for a multi-part item.

    parts: iterable of (plex_file_path, size_bytes).
    Returns:
      (tier_str, breakdown)
      tier_str: 'HOT' | 'WARM' | 'MIXED' | 'UNKNOWN'
      breakdown: dict of per-tier byte shares (0.0..1.0)

    Decision rules:
      - Majority (>50%) bytes on HOT  -> HOT
      - Majority bytes on WARM        -> WARM
      - Majority bytes UNKNOWN        -> UNKNOWN
      - Otherwise (50/50 tie, or no clear majority) -> MIXED
    """
    totals = {"HOT": 0, "WARM": 0, "UNKNOWN": 0}
    total = 0
    for plex_path, size in parts:
        if not size or size <= 0:
            continue
        total += size
        translated = translate_plex_path(plex_path or "", path_map)
        resolved = resolve_user_share(translated, user_share_prefix, hot_mount, array_disks)
        tier = classify_path(resolved, hot_mount, array_disks)
        totals[tier] += size
    if total == 0:
        return "UNKNOWN", {"HOT": 0.0, "WARM": 0.0, "UNKNOWN": 0.0}
    split = {k: round(v / total, 4) for k, v in totals.items()}
    if split["HOT"] > 0.5:
        return "HOT", split
    if split["WARM"] > 0.5:
        return "WARM", split
    if split["UNKNOWN"] > 0.5:
        return "UNKNOWN", split
    return "MIXED", split


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

        # Current tier (P1). Probes only if tier detection is configured.
        current_tier = "UNKNOWN"
        tier_split: Optional[dict] = None
        if tier_probing:
            parts = []
            for m in group:
                parts.extend(_media_parts(getattr(m, "media", None)))
            current_tier, tier_split = resolve_item_current_tier(
                parts, path_map, hot_mount, array_disks, user_share_prefix,
            )
            if tier_split:
                breakdown["tier_split"] = tier_split

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
            outcome="NEUTRAL",  # finalised in post-scoring override pass
            score_breakdown=breakdown,
        )


def collect_series(
    section, library_name: str, now, thresholds, history_index: dict,
    path_map, hot_mount: str, array_disks: List[str],
    user_share_prefix: str = "",
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

        # Current tier (P1): majority-bytes rollup across every episode.
        current_tier = "UNKNOWN"
        tier_split: Optional[dict] = None
        if tier_probing:
            parts = []
            for ep in episodes:
                parts.extend(_media_parts(getattr(ep, "media", None)))
            current_tier, tier_split = resolve_item_current_tier(
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
            outcome="NEUTRAL",  # finalised in post-scoring override pass
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
      1. Library pin       -> HOT, pinned=True
      2. Title pin         -> HOT, pinned=True
      3. Recency floor     -> HOT if last_played within hot_recency_days
                              AND raw recommendation is NEUTRAL or WARM.
      4. Raw score         -> HOT / WARM / NEUTRAL via score_recommendation.
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

    # --- 3. Raw score ---
    raw_rec = score_recommendation(item.score, thresholds)

    # --- 4. Recency floor (only promotes, never demotes) ---
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
            items.extend(
                collect_movies(
                    section, name, now, cfg["thresholds"], history_index,
                    path_map, hot_mount, array_disks, user_share_prefix,
                )
            )
        elif section.type == "show":
            items.extend(
                collect_series(
                    section, name, now, cfg["thresholds"], history_index,
                    path_map, hot_mount, array_disks, user_share_prefix,
                )
            )
        else:
            print(
                f"! Library '{name}' has unsupported type '{section.type}', skipping",
                file=sys.stderr,
            )

    # Post-scoring override pass. Done here (not per-collector) so both
    # movie and series paths share the same rule engine and the summary
    # counts reflect final outcomes.
    for it in items:
        _apply_overrides(it, cfg, now)

    return items


# ---------- Output ----------


def _fmt_date(d: Optional[datetime]) -> str:
    return d.strftime("%Y-%m-%d") if d else "—"


def _fmt_size(gb: float) -> str:
    if gb >= 1000:
        return f"{gb / 1024:.2f} TB"
    return f"{gb:.1f} GB"


# Outcomes that map to each projected tier if every recommendation were
# executed. The bucket reflects where the item will END UP, not where it
# currently sits — so TO_HOT + STAY_HOT + PIN_HOT all go into HOT.
_HOT_OUTCOMES = {"SHOULD_BE_HOT", "PIN_HOT", "STAY_HOT", "TO_HOT"}
_WARM_OUTCOMES = {"SHOULD_BE_WARM", "STAY_WARM", "TO_WARM"}
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
        help="(P2+) execute moves — NOT YET IMPLEMENTED, will error out",
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

    if args.apply:
        log.error(
            "--apply is not implemented in P0 (read-only). "
            "Ship P1 (tier detection) and P2 (rsync moves) first."
        )
        return 3

    plex = connect_plex(cfg["plex"]["url"], cfg["plex"]["token"], notifier, ncfg)

    items = collect_all(plex, cfg, filter_libraries=args.library)
    items = apply_sort(items, args.sort)
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


if __name__ == "__main__":
    if "--_test" in sys.argv:
        _test_resolve_user_share()
        sys.exit(0)
    sys.exit(main())
