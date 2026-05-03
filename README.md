# plex-media-tiering — Storage Tiering Script for Plex media

`tier.py` scores every movie and TV series in your Plex libraries based on
watch history and age, and recommends whether each item should sit on the HOT
tier (HDD ZFS pool) or the WARM tier (Unraid parity array). Scheduled to run
monthly or when the ZFS pool fills past a threshold.

**Current status: Phase P2.2 + P2.3 — all three move directions.** The script
connects to Plex, computes scores, detects the current tier of each item, and
(when `moves.enabled: true`) dry-runs or executes rsync moves in all three
directions: TO_HOT (warm array → hot pool), TO_WARM (hot pool → warm array),
and RELOCATE_WARM (evicting warm disk → healthy warm disk). Pass `--apply` to
execute; default is dry-run.

## Phase roadmap

| Phase | What it does | Status |
|-------|--------------|--------|
| P0 | Plex connect + scoring + table output. Read-only. | **Done** |
| P0.1 | Pinning (library + title), recency floor, projected-tier footer. | **Done** |
| P0.2 | Auto-inherit — when ≥N members of a collection naturally score HOT, promote the whole collection. | **Done** |
| P0.3 | Collection pinning — force every member of a named Plex collection to HOT. | **Done** |
| P0.4 | Added-date floor — promote recently-added movies and TV shows with fresh episodes to HOT. | **Done** |
| P0.5 | Disk eviction — mark warm-tier array disks as evicting; items on them get `RELOCATE_WARM`. Data model + reporting only; actual moves are P2. | **Done** |
| P1 | Filesystem probing to detect current tier. Auto-detect array disks + Plex path translation. Majority-bytes rollup. | **Done** |
| P2.1 | Move executor — TO_HOT direction. rsync from warm array to hot pool. Dry-run by default; `--apply` executes. | **Done** |
| P2.2 | TO_WARM moves — demote items from hot pool to chosen warm disk (co-location + most-free selection). | **Done** |
| P2.3 | RELOCATE_WARM moves — drain evicting warm disks to healthy warm disks. Evicting disk excluded from candidates. | **Done** |
| P2.4 | Plex rescan automation — **intentionally not implemented**. Unraid's user-share union means Plex always reads through `/mnt/user/` regardless of which physical disk backs a file; TO_WARM and RELOCATE_WARM moves are invisible to Plex at the path level. Only TO_HOT (which moves files off the union to a separate ZFS mount) recommends a rescan. | N/A |
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

| Event | Tags produced |
| --- | --- |
| Push to `main` | `:latest`, `:main`, `:sha-<short>` |
| Push to other branch (e.g. `dev`) | `:<branch>`, `:sha-<short>` |
| Pull request (same-repo) | `:pr-<N>`, `:sha-<short>` |
| Git tag `v1.2.3` | `:1.2.3`, `:1.2`, `:1`, `:latest`, `:sha-<short>` |

- `dazzathewiz/plex-media-tiering:latest` — stable, built from `main`.
  Point Unraid CA here for rolling updates.
- `dazzathewiz/plex-media-tiering:pr-<N>` — per-PR test image, useful for
  manually testing a fix before merging.
- `dazzathewiz/plex-media-tiering:dev` — rolling dev branch tip, if you
  push a `dev` branch.
- `:1.2.3` / `:1.2` / `:1` — created by pushing a `v1.2.3` git tag.
  Point CA at `:1` for patch+minor auto-updates without breaking
  changes.

Fork PRs (external contributors): the workflow builds the image for CI
validation but does not push to Docker Hub (secrets unavailable).

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

`--apply` activates move execution when `moves.enabled: true`. Without it the move pass runs in dry-run mode.

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
| `RELOCATE_WARM` | On a WARM disk marked for eviction. Score says keep warm, but P2 must move to a different warm disk. See [Disk eviction](#disk-eviction) below. |

Tier detection activates automatically when `paths.hot_pool_mount` is set AND
at least one array disk is known (either listed explicitly in `paths.array_disks`
or found by auto-detect). If either is missing, outcomes degrade to
`SHOULD_BE_*` / `NEUTRAL` so the script stays useful in a bare-P0 config.

### Overrides: pinning + recency + added floors

Seven-step precedence applied after scoring (highest wins):

1. **`pinning.always_hot_libraries`** — list of library names (case-insensitive
   exact match). Every item in those libraries is `PIN_HOT`. Useful for things
   like a 4K library where the whole point is to serve from the fast tier.
2. **`pinning.always_hot_titles`** — list of title substrings (case-insensitive).
   Any item whose title contains one of these is `PIN_HOT`. Great for long-tail
   favourites — pinning `"Stargate"` catches SG-1, Atlantis, Universe, Origins
   and the movie in a single entry.
3. **`pinned_collections`** — explicit collection pin. See below.
4. **`auto_collection_inherit`** — automatic collection pin. See below.
5. **Added-date floor** — promote-only, never demotes. See below.
6. Raw score → HOT / WARM / NEUTRAL.
7. **`thresholds.hot_recency_days`** — recency floor. If a show or movie was
   last played within this window, it's promoted to HOT even if the raw score
   lands in NEUTRAL or WARM. This catches infrequent-but-active shows (one
   episode every few weeks) that the play-weighted score otherwise demotes.
   Default: `730` (≈2 years). Set to `0` or comment out to disable.

#### Choosing between override layers

Three mechanisms can promote items to HOT beyond their raw score. Pick the
right tool for each use case:

- **`pinning.always_hot_titles`** — you know a specific title or franchise by
  name and you always want it on the fast tier, regardless of how much you've
  watched it. Zero setup overhead; works across libraries.

- **`pinned_collections`** — you maintain a Plex collection (e.g. "Must
  Watch", "Kids favourites") and want every member kept HOT. Explicit: you
  curate the collection, tier.py pins it unconditionally.

- **`auto_collection_inherit`** — you don't want to curate a list, but you
  notice you've been actively watching part of a franchise and want the rest
  ready. Auto-inherit detects that ≥N members scored HOT naturally and
  promotes the whole collection. No YAML required beyond enabling the feature.
  Default off — enable once you've validated your scoring thresholds.

All three produce `outcome=PIN_HOT` and appear in `--explain` output.

Pinning and floor promotions tag the `score_breakdown` with an `override` key
so `--explain` shows why the item was promoted past its raw score.

#### Collection pinning

Force every member of a named Plex collection to HOT, regardless of score.
Useful for hand-curated "always keep hot" groups — an MCU collection, a
"Kids favourites" collection, a "Currently watching" collection managed from
the Plex UI.

```yaml
pinned_collections:
  - library: "Movies"
    name: "Marvel Cinematic Universe"
  - library: "TV Shows"
    name: "Must Watch"
```

Both `library` and `name` are required per entry. `library` is case-sensitive
(must match the Plex library name exactly). `name` is case-insensitive exact
match against the collection title. Members show `outcome=PIN_HOT`. An empty
list (the default) disables collection pinning entirely.

If a collection or library isn't found, tier.py logs a WARNING and continues —
a typo in config won't abort a scheduled run.

The footer line `Collection-pin promotions: N items` counts how many items were
pinned by this rule.

#### Auto-inherit collection pin

Automatically promotes all members of a Plex collection to HOT when enough of
its members already score HOT naturally (pre-floor, pre-pin). Designed for the
case where you've been watching part of a franchise and want the rest pre-warmed
without manually curating a config entry.

```yaml
auto_collection_inherit:
  enabled: true
  min_hot_members: 2          # 2+ naturally-hot members triggers the whole collection
  skip_smart_collections: true  # skip rules-based smart collections
  exclude_libraries: []       # libraries to exempt (e.g. ["DVD Rips"])
```

How it works:

1. After scoring, tier.py scans all configured Plex libraries (minus
   `exclude_libraries`) for collections.
2. For each collection, it counts members whose `score >= score_to_hot`
   (natural score, before any floor or pin override).
3. If that count reaches `min_hot_members`, every member of the collection is
   promoted to HOT — including cold members that scored WARM or NEUTRAL on
   their own.

`min_hot_members: 2` (default) means one popular entry in a franchise won't
drag the rest along — it takes at least two to infer genuine engagement with
the set. `min_hot_members: 1` removes that guard if you prefer more aggressive
promotion.

**Smart collections** are rules-based playlists Plex manages automatically
(e.g. "Recently Added", "Top Rated"). Skipping them (`skip_smart_collections:
true`, the default) avoids false positives from automated curation. Set to
`false` if you have smart collections you've intentionally set up for tiering.

Log lines added when enabled:

```text
Auto-inherit: 4 collections triggered (≥2 hot members), 38 items inherited
  Auto-inherit promotions: 22 items
```

The first line appears during collection (how many collections triggered and
how many total member slots were inherited). The second is in the run summary
(how many items actually changed outcome — the diff from the first number is
items that were already going to be HOT).

**Precedence with explicit collection pin**: if an item belongs to both a
`pinned_collections` entry and a triggered auto-inherit collection, the
explicit pin takes precedence and the item is counted in the
`Collection-pin promotions` counter, not `Auto-inherit promotions`.

#### Added-date floor

Items added to Plex recently are likely to be watched regardless of their
release year. The added-date floor promotes them to HOT without requiring any
play history.

- **Movies**: if `movie.addedAt` is within `thresholds.added_floor_days_movies`
  days, the movie is promoted to HOT. Default: `45`.
- **TV shows**: if any episode in the show was added within
  `thresholds.added_floor_days_tv` days, the show is promoted to HOT. This
  catches currently-airing series whose show-level `addedAt` is old but whose
  episodes are fresh. Default: `30`.

Set either threshold to `0` or `null` to disable it for that media type.

```yaml
thresholds:
  added_floor_days_movies: 45   # movies added within 45 days -> HOT
  added_floor_days_tv: 30       # TV show with episode added within 30 days -> HOT
```

The footer line `Added-floor promotions: N items` in the run log counts how
many items were promoted by this rule.

## Disk eviction

Mark one or more warm-tier array disks as "evicting" so items on them appear in
the report as needing relocation — regardless of their tier score.

**Use cases:**

- A disk developing reallocated sectors that needs to retire before failure.
- A disk with cabling or firmware issues you want to drain before debugging.
- Rebalancing media across array disks to reclaim space or even wear.

Eviction is orthogonal to the hot/warm tier decision. An item flagged
`RELOCATE_WARM` isn't on the wrong *tier* — it's on the wrong *disk within
that tier*. Its score still says WARM; only its current physical location is
unacceptable.

**Eviction does not move data — it flags items so the dry-run report shows
what would move when P2's mover lands.**

```yaml
array_disk_evict:
  enabled: true
  disks:
    - "/mnt/disk7"   # disk developing bad sectors — drain before retirement
    - "/mnt/disk2"   # cabling issues — drain before debug
```

Disk paths must match the format used in `paths.array_disks` exactly
(case-sensitive). A typo logs a `WARNING` per bad entry and is skipped; the
run continues with the valid entries.

### How outcomes change under eviction

| Natural outcome on the evicting disk | Eviction outcome | Why |
|---|---|---|
| `STAY_WARM` | `RELOCATE_WARM` | Score says warm but P2 must pick a different warm disk. |
| `TO_HOT` | `TO_HOT` (unchanged) | Move to hot resolves the eviction implicitly — no new outcome needed. |
| `PIN_HOT` / `SHOULD_BE_HOT` | unchanged | Already going to HOT; eviction is resolved. |
| Anything with `current_tier = HOT` | unchanged | Hot-tier eviction is out of scope for v1 (ZFS handles drive replacement). |

### Log lines

When enabled with at least one valid disk, two log lines appear per run:

```text
Eviction: 2 disks marked (/mnt/disk2, /mnt/disk7), 47 items currently on evicting disks
Eviction: 31 items flagged RELOCATE_WARM (TO_HOT path: 16)
```

The first line shows scope. The second shows how items split: 31 need explicit
relocation by P2's mover, 16 are already going to HOT and resolve the eviction
implicitly. If `enabled: false` or the disk list is empty, no `Eviction:` lines
appear.

`RELOCATE_WARM` counts in the projected **WARM** size total in the run
summary — these items are staying warm, just moving disks within that tier.

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

## Moves (P2)

The move executor (P2.1) turns `TO_HOT` recommendations into actual rsync
operations. It is opt-in and safe by default.

### Enabling

```yaml
moves:
  enabled: true
```

With `enabled: false` (the default), the move pass is skipped entirely — the
dry-run analysis table still prints as usual.

### Dry-run vs apply

**Default (no `--apply`)** — dry-run. A `[DRY-RUN]` block logs every
projected move with source path, destination path, size, and an ETA estimate
at 200 MB/s throughput. No filesystem changes are made.

```
[DRY-RUN] Moves: TO_HOT=3 — 12.3 GB total — estimated 1m02s at 200 MB/s
[DRY-RUN]   Foo (2010) — 4.2 GB — /mnt/disk4/Movies/Foo (2010) → /mnt/hot_pool/Movies/Foo (2010)
...
```

**With `--apply`** — executes moves serially. Each item is rsynced, size-verified,
and (if configured) source-deleted. A per-item `[SUCCESS]` / `[SKIPPED]` /
`[FAILED]` line is logged with the elapsed time.

**Scheduled runs (no CLI access)** — set `moves.apply: true` in `tiering.yaml`
instead of passing `--apply` on the command line. The two flags are additive;
either alone is sufficient to trigger apply mode.

```yaml
moves:
  enabled: true
  apply: true   # execute moves without --apply on the CLI
```

### Scope

All three move directions are active:

| Direction | Source | Destination | Trigger |
| --- | --- | --- | --- |
| `TO_HOT` | Warm array disk | Hot pool | Score above `score_to_hot`, recency floor, or pin |
| `TO_WARM` | Hot pool | Warm array disk | Score drops below `score_to_warm` |
| `RELOCATE_WARM` | Evicting warm disk | Healthy warm disk | Item on a disk listed in `array_disk_evict.disks` |

Items with outcome `SHOULD_BE_HOT` or `SHOULD_BE_WARM` (tier detection
inactive) are reported but not moved.

**Warm disk selection** (`TO_WARM` and `RELOCATE_WARM`): tier.py picks the
destination warm disk using the `co_locate_then_most_free` strategy:

1. Exclude the source disk (RELOCATE_WARM) or no exclusion (TO_WARM).
2. Filter candidates by free space ≥ item size + `safety_margin_gb`.
3. Among qualified disks, prefer the one that already holds the most bytes of
   this item (co-location — keeps a series on one spindle for efficient binging).
4. Fallback: most-free disk.

**Minority-evict handling**: if a series has its majority bytes on a safe disk
but a season or two on an evicting disk, only those evicting-disk files are
moved. The majority files on the safe disk are untouched and the move
co-locates with the safe disk automatically.

**Straggler cleanup**: after scoring, items that are majority-on-HOT but still
have files on warm disks (`STAY_HOT + warm stragglers`) are automatically
promoted to `TO_HOT` to finish the migration. Similarly, items pinned to HOT
(`PIN_HOT`) that physically live on a warm disk are converted to `TO_HOT` so
the pin takes effect.

### Source deletion

```yaml
moves:
  delete_source_after_verify: true   # default
  size_verify: true                  # default
```

Source files are only deleted if:

1. rsync exits 0, **and**
2. `size_verify: true` and the destination byte count matches source within 1 KB.

Verification is done per-file using `os.path.getsize`, measuring only the
files that were moved. Pre-existing files already on the hot pool (for
partially-migrated MIXED-tier items) are not counted, avoiding false failures.

After deletion, empty ancestor directories (season folders, show folders, year
folders, etc.) are automatically pruned up to but not including the disk root.
Non-empty directories are left untouched.

Set `delete_source_after_verify: false` for the first few apply runs to keep
the source as a safety net. The item will exist in both locations until you
remove the source manually.

Sidecar files (`.nfo`, `.srt`, `.sub`, language-tagged subtitles like
`Movie.en.srt`) are moved alongside the media file automatically — Plex
discovers them by directory scan at play time, so they must live on the same
tier as the media.

### Parity-check guard

```yaml
moves:
  parity_check_blocking: true   # default
```

Before starting any moves, tier.py reads `/proc/mdstat` to detect whether an
Unraid parity check or resync is running. With `parity_check_blocking: true`
(the default), the move pass aborts before any rsync call. Set `false` for an
escape hatch if you know what you're doing.

### Bandwidth throttle

```yaml
moves:
  bandwidth_limit_mbps: 50   # limit rsync to 50 MB/s
```

Passes `--bwlimit` to rsync. `null` (default) = unthrottled.

### Recommended first-run sequence

1. Set `moves.enabled: true`, leave `--apply` off. Run and check the
   `[DRY-RUN]` block looks correct.
2. Set `delete_source_after_verify: false`. Run with `--apply` on a small test
   item. Confirm the destination has the file and the source still exists.
3. After watching a few successful apply runs, set
   `delete_source_after_verify: true`.

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
