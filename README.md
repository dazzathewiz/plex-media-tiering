# plex-media-tiering — Storage Tiering Script for Plex media

`tier.py` scores every movie and TV series in your Plex libraries based on
watch history and age, and recommends whether each item should sit on the HOT
tier (HDD ZFS pool) or the WARM tier (Unraid parity array). Scheduled to run
monthly or when the ZFS pool fills past a threshold.

**Current status: Phase P1 (read-only).** The script connects to Plex, computes
scores, detects the current tier of each item by probing the filesystem, and
prints the analysis table. It still doesn't move anything — that's P2.

## Phase roadmap

| Phase | What it does | Status |
|-------|--------------|--------|
| P0 | Plex connect + scoring + table output. Read-only. | **Done** |
| P0.1 | Pinning (library + title), recency floor, projected-tier footer. | **Done** |
| P1 | Filesystem probing to detect current tier. Auto-detect array disks + Plex path translation. Majority-bytes rollup. | **Done** |
| P0.2 | Collection-aware grouping (Harry Potter, Hunger Games, etc.). | Pending |
| P2 | `--apply` with rsync moves + Plex rescan. | Pending |
| P3 | Hardened safeguards (lock file, currently-playing skip, free-space check, move-size cap). | Pending |
| P4 | Scheduled cron + size-triggered wrapper. | Pending |

## Install

### Test-first — local builds on Unraid (no Docker Hub needed)

Recommended for the first few runs. Iterate freely without touching
Docker Hub at all.

**Path A — script only (fastest loop):**
```
mkdir -p /boot/config/plugins/user.scripts/scripts/plex-media-tiering
cd       /boot/config/plugins/user.scripts/scripts/plex-media-tiering
# Copy tier.py, requirements.txt, example.tiering.yaml here; rename the
# last one to tiering.yaml and paste your Plex token.

python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# Dry run — writes the log next to the script for easy viewing
./venv/bin/python tier.py \
    --config ./tiering.yaml \
    --log-file ./plex-media-tiering.log \
    --top 30
```

**Path B — build the container locally (validates the whole stack):**
```
# Copy the entire plex-media-tiering/ folder to the Unraid host
cd /mnt/user/appdata/plex-media-tiering-build    # or wherever you stashed it

docker build -t plex-media-tiering:dev .

# Create the config dir + drop tiering.yaml in
mkdir -p /mnt/user/appdata/plex-media-tiering
cp example.tiering.yaml /mnt/user/appdata/plex-media-tiering/tiering.yaml
# edit /mnt/user/appdata/plex-media-tiering/tiering.yaml, paste token

docker run --rm \
    -v /mnt/user/appdata/plex-media-tiering:/config \
    -v "/mnt/user/TV Shows:/data/tv:ro" \
    -v "/mnt/user/Movies:/data/movies:ro" \
    -v /mnt/hot_pool:/mnt/hot_pool:ro \
    --network host \
    plex-media-tiering:dev --top 30
```

The image stays on the host. No registry involved. Once it's behaving,
push to Docker Hub (below) and swap the CA template over.

### Recommended — Docker container via Community Applications

This is the production path. Image is built by GitHub Actions on every push
to `main` and published to Docker Hub; Unraid's CA auto-update keeps it
current.

1. Copy `unraid-template.xml` from this repo to
   `/boot/config/plugins/dockerMan/templates-user/my-plex-media-tiering.xml` on your
   Unraid host (or submit it to Community Applications).
2. Edit the template and replace `REPLACE_WITH_DOCKERHUB_USERNAME` with
   your Docker Hub username. Set the hot-pool mount to your actual pool
   name.
3. Add the container from the Docker tab. The default mounts mirror a
   Plex container so share layout stays consistent.
4. Edit `/mnt/user/appdata/plex-media-tiering/tiering.yaml` (copy from
   `example.tiering.yaml`) — paste your Plex token, confirm the library
   names, point `paths.hot_pool_mount` at your ZFS pool.
5. Test: `docker start plex-media-tiering` from the Unraid GUI or
   `docker start -a plex-media-tiering` from a terminal to see the output live.
6. Schedule via User Scripts — the one-line wrapper is just:
   ```
   #!/bin/bash
   /usr/bin/docker start plex-media-tiering
   ```
   Set it to monthly, aligned with your parity check window.

The container exits after it prints its report. That's intentional — it's a
one-shot scheduled job, not a long-running service.

### Alternative — venv on /boot (bare install)

For hacking on the script without the container rebuild loop:

```
mkdir -p /boot/config/plugins/user.scripts/scripts/plex-media-tiering
cd       /boot/config/plugins/user.scripts/scripts/plex-media-tiering
# copy tier.py, example.tiering.yaml -> tiering.yaml, requirements.txt

python3 -m venv venv
./venv/bin/pip install -r requirements.txt

./venv/bin/python tier.py --config ./tiering.yaml --top 30
```

The venv survives reboots because `/boot` is the USB stick. Do NOT install
to host Python — Unraid boots its OS into RAM and the install is wiped on
reboot.

### Plex token

Generate once, paste into `tiering.yaml`:
<https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/>

Tokens are long-lived — only re-issue if you change your Plex password,
manually revoke devices, or unlink the server. Keep `tiering.yaml` at
`chmod 600`.

**If the token goes stale:** the script detects it on connect and fires an
alert (see Notifications below) so you're not flying blind.

## Docker Hub + GitHub Actions (publish pipeline)

**Free-tier compatible.** Docker Hub free gives unlimited public repos;
GitHub Actions is free on public repos. The workflow builds multi-arch
(amd64 + arm64) on every push to `main` and every semver tag.

**Secrets setup (one-time):**
1. Docker Hub → Account Settings → Security → **New Access Token**.
   Scope = Read, Write, Delete. Copy the token.
2. GitHub repo → Settings → Secrets and variables → Actions:
   - `DOCKERHUB_USERNAME` = your Docker Hub username
   - `DOCKERHUB_TOKEN` = the PAT from step 1
3. Optional: `DOCKERHUB_IMAGE` variable (not secret) if you want a
   different image name than `<username>/plex-media-tiering`.

**What's safe:**
- Plex token lives in `tiering.yaml` on the Unraid host. `.dockerignore`
  excludes it from the build context, so it can never leak into a pushed
  image layer.
- GitHub Actions secrets are masked in workflow logs.
- The PAT is Docker Hub-scoped; if it leaks, revoke it on Hub without
  touching your account password.

**Tagging strategy:**
- `:latest` and `:main` — moving tags, auto-updated on every main push.
  Fine for "track tip-of-branch" during development.
- `:1.2.3` / `:1.2` / `:1` — created by pushing a `v1.2.3` git tag.
  Point CA at `:1` for patch+minor auto-updates without breaking
  changes.

## Logging

Human-readable rotating log at `/config/tier.log` (inside the container) =
`/mnt/user/appdata/plex-media-tiering/tier.log` on the Unraid host. Viewable from the
Unraid GUI file manager, any SMB mount, or `docker logs plex-media-tiering`.

Log format:

```
2026-04-18 14:22:10 INFO    tier.py starting — config=/config/tiering.yaml
2026-04-18 14:22:10 INFO    Connecting to Plex at http://localhost:32400
2026-04-18 14:22:11 INFO    Plex OK: server='Plex'
2026-04-18 14:22:14 INFO    Run summary: 847 items  total=24.3 TB
2026-04-18 14:22:14 INFO      Projected HOT      124 items  3.82 TB
2026-04-18 14:22:14 INFO      Projected WARM     302 items  18.14 TB
2026-04-18 14:22:14 INFO      Projected NEUTRAL  421 items  2.34 TB
2026-04-18 14:22:14 INFO      Outcome counts: NEUTRAL=421  PIN_HOT=35  SHOULD_BE_HOT=89  SHOULD_BE_WARM=302
```

Rotation: 2 MB per file, 5 backups retained. Tune via `logging:` in
`tiering.yaml`.

### Viewing logs

- **Unraid File Manager tab** (built in since 6.12) — navigate to
  `appdata/plex-media-tiering/tier.log`, preview in-browser. Easiest for quick checks.
- **`docker logs plex-media-tiering`** from the Docker tab → pencil icon → Log. Shows
  all INFO lines including the startup messages and the projected-tier footer.
- **SMB share** — `\\<your-unraid-host>\appdata\plex-media-tiering\tier.log` if you
  prefer a desktop text editor.
- **P5 future:** a small built-in HTTP log viewer exposed on a container
  port (`WebUI` slot in the CA template) — not worth building until
  `--apply` is live and we actually need to glance at moves from any
  device without SSH.

## Notifications

Three conditions fire an alert:

| Condition | When | Config key |
|---|---|---|
| Plex token invalid | Server returns 401 | `on_auth_failure` |
| Plex unreachable | Connection refused / timeout | `on_plex_unreachable` |
| Script crashed | Unhandled exception | `on_script_error` |

Two delivery channels (both optional, use either or both):

### Webhook
Generic JSON POST — works with Home Assistant webhooks, gotify, ntfy,
Discord (via a shape relay), or anything else that speaks JSON. Body:

```json
{
  "source": "tier",
  "level": "error",
  "title": "Tier: Plex token invalid",
  "message": "tier.py could not authenticate to Plex — the token was rejected...",
  "timestamp": "2026-04-18T14:22:10+00:00"
}
```

Set `notifications.webhook.url` in `tiering.yaml`. Auth header optional.

### Unraid notifier
Uses Unraid's built-in `/usr/local/emhttp/webGui/scripts/notify` so alerts
show in the bell dropdown AND go to whatever agents you've configured
(email, pushover, etc.).

Container installs need the notify script bind-mounted in — the
Community Apps template includes that mount as an "advanced" option.

## Usage

```bash
# Default: read-only analysis, sorted by score desc
./tier.py

# Top 25 items by score
./tier.py --top 25

# Just the movies
./tier.py --library "Movies"

# Emit CSV for spreadsheet review
./tier.py --csv /tmp/tiering.csv

# Scoring breakdown for a specific title
./tier.py --explain "The Expanse"

# Machine-readable output
./tier.py --json

# Logging overrides
./tier.py --log-level DEBUG
./tier.py --log-file /tmp/tier.log
./tier.py --quiet           # no console output, file log only
```

`--apply` exists but errors out in P0 (read-only phase).

## Scoring

```
score = play_weight + age_grace_weight

play_weight      = log2(1 + plays) * 20 * recency_factor
recency_factor   = exp(-days_since_last_play / 90)   (0 if never played)
age_grace_weight = score_to_warm + 5 if never_played AND age < 180 days else 0
```

(The grace weight sits just above `score_to_warm` so fresh unwatched items
land in the NEUTRAL dead zone, not in SHOULD_BE_WARM on day 1.)

Outcomes:

| Outcome | Meaning |
|---|---|
| `PIN_HOT` | Pinned by library or title override. If on HOT already, no move; if elsewhere, P2 will promote. |
| `STAY_HOT` | Already on HOT, score keeps it there. No move. |
| `TO_HOT` | On WARM (or MIXED) but the score / recency floor says HOT. P2 will promote. |
| `SHOULD_BE_HOT` | Score says HOT but current tier is UNKNOWN (tier detection is off or the file path didn't resolve). Fix `plex_path_map` to upgrade this to STAY_HOT / TO_HOT. |
| `STAY_WARM` | Already on WARM, score keeps it there. No move. |
| `TO_WARM` | On HOT (or MIXED) but the score says WARM. P2 will demote. |
| `SHOULD_BE_WARM` | Score says WARM but current tier is UNKNOWN. |
| `NEUTRAL` | In the score dead zone AND tier detection disabled. |
| `MIXED_NEUTRAL` | Files are split exactly 50/50 across tiers with no score-based direction to resolve it. P2 leaves it alone. Very rare. |

Tier detection activates automatically when `paths.hot_pool_mount` is set AND
at least one array disk is known (either listed explicitly in `paths.array_disks`
or found by auto-detect). If either is missing, outcomes degrade to
`SHOULD_BE_*` / `NEUTRAL` so the script stays useful in a bare-P0 config.

### Overrides: pinning + recency floor

Three ways to force HOT regardless of raw score, evaluated in this order:

1. **`pinning.always_hot_libraries`** — list of library names (case-insensitive
   exact match). Every item in those libraries is `PIN_HOT`. Useful for things
   like a 4K library where the whole point is to serve from the fast tier.
2. **`pinning.always_hot_titles`** — list of title substrings (case-insensitive).
   Any item whose title contains one of these is `PIN_HOT`. Great for long-tail
   favourites — pinning `"Stargate"` catches SG-1, Atlantis, Universe, Origins
   and the movie in a single entry.
3. **`thresholds.hot_recency_days`** — recency floor. If a show or movie was
   last played within this window, it's promoted to `SHOULD_BE_HOT` even if
   the raw score lands in `NEUTRAL` or `SHOULD_BE_WARM`. This catches
   infrequent-but-active shows (one episode every few weeks) that the play-
   weighted score otherwise demotes. Default: `730` (≈2 years). Set to `0` or
   comment out to disable.

Pinned and recency-floor promotions tag the `score_breakdown` with an
`override` key so `--explain` shows why the item was promoted past its raw
score.

## Tier detection (P1)

Tier detection classifies each item's current physical location — HOT pool
vs WARM array — so the script can emit real move directives (`STAY_HOT`,
`TO_HOT`, `STAY_WARM`, `TO_WARM`) instead of just recommendations.

Three pieces of config control it:

- **`paths.hot_pool_mount`** — the mount point of your fast pool as tier.py
  sees it (e.g. `/mnt/hot_pool`). Required.
- **`paths.array_disks`** — optional explicit list of WARM disks. Empty list
  triggers auto-detect of every `/mnt/disk[0-9]*` that is an actual mount
  point visible to the container. Populate to restrict the set.
- **`paths.array_disk_exclude`** — drives to skip from auto-detect (e.g. a
  disk you're retiring).
- **`paths.plex_path_map`** — Plex reports file paths as it sees them. If
  Plex runs in a separate container / VM / host, those paths won't resolve
  on the host running tier.py. Map each Plex-side prefix to the matching
  tier-side prefix. Longest prefix wins on each file.

Example for a Plex container that mounts media at `/data/...` while tier.py
sees the same media at `/mnt/user/...`:

```yaml
paths:
  hot_pool_mount: "/mnt/hot_pool"
  plex_path_map:
    - plex: "/data/movies"
      tier: "/mnt/user/Movies"
    - plex: "/data/tv"
      tier: "/mnt/user/TV Shows"
```

### Multi-part rollup

Movies and series can have files spread across tiers (multi-version uploads,
mid-migration state). tier.py rolls each item up by **majority of bytes**:

- > 50% of bytes on HOT → `current_tier = HOT`
- > 50% on WARM → `current_tier = WARM`
- > 50% UNKNOWN (unresolved paths) → `current_tier = UNKNOWN`
- 50/50 tie → `current_tier = MIXED`

The per-tier byte split is recorded in the `score_breakdown` under
`tier_split` so `--explain` can show exactly why an item was classified
the way it was.

### When tier detection fails

If every file path resolves to UNKNOWN (common cause: `plex_path_map` isn't
configured yet and Plex is reporting paths tier.py can't see), the item
stays `current_tier = UNKNOWN` and its outcome degrades to `SHOULD_BE_*`
rather than `STAY_*`/`TO_*`. Look for `current_tier: UNKNOWN` in
`--explain` output — that's the tell.

## Tuning

All thresholds live in `tiering.yaml`. After the first few runs, look for:
- Items with obviously-wrong outcomes and adjust `score_to_hot` / `score_to_warm`
- `age_grace_days` if newly-added items are demoted too quickly
- `recency_half_life_days` if the "hotness" of recent plays decays too fast/slow
- `hot_recency_days` if items you know you watch are still being marked warm
  (shorten it if too many items qualify; lengthen if favourites are being demoted)
- `pinning.always_hot_titles` / `always_hot_libraries` for explicit
  "never-demote" items where no amount of threshold tuning should affect them

Use `--explain TITLE` to see the scoring math for any single item. Pinned and
recency-floor promotions appear under the `override` key in the breakdown.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Config error (file missing, token placeholder, empty libraries) |
| 2 | Plex unreachable or auth failed |
| 3 | `--apply` used in P0 (not yet implemented) |
| 4 | Unhandled runtime error (notification fired if configured) |
| 130 | Interrupted (SIGINT) |

## Repo layout (when published)

```
tier/
├── tier.py                      # the script
├── requirements.txt             # plexapi, pyyaml
├── example.tiering.yaml         # config template
├── Dockerfile                   # python:3.12-slim + rsync + the script
├── .dockerignore
├── unraid-template.xml          # Community Applications template
├── .github/workflows/
│   └── docker-publish.yml       # GH Actions -> Docker Hub multi-arch
├── README.md
└── LICENSE
```
