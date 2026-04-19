# Agent guide — plex-media-tiering

Read this before touching the code. It captures decisions that are easy to
re-litigate (and get wrong) from reading the diff alone.

## What this repo is

A single-file Python script (`tier.py`) that asks Plex which media is
hot/cold, probes the local filesystem to see where each item currently
lives, and recommends where it *should* live — on a fast pool (HOT) or a
parity array (WARM). Phased design; it refuses to actually move anything
until P2 ships.

Runs as a one-shot Docker container scheduled by Unraid's User Scripts
plugin. Image is built by GitHub Actions on every push to `main`, multi-
arch (amd64 + arm64), published to Docker Hub.

## Repo layout

```
.
├── tier.py                       # the script (single file, no package)
├── example.tiering.yaml          # config template — auto-seeds /config on first run
├── requirements.txt              # plexapi, pyyaml
├── Dockerfile                    # python:3.12-slim + rsync + the script
├── .dockerignore                 # keeps tiering.yaml + secrets OUT of image layers
├── unraid-template.xml           # Community Applications template
├── .github/workflows/
│   └── docker-publish.yml        # multi-arch build, pushes tags + latest
├── README.md                     # end-user docs
├── LICENSE                       # MIT
└── AGENTS.md                     # this file
```

CLAUDE.md and GEMINI.md are symlinks to this file.

## Phases

| Phase | Scope | Status |
|---|---|---|
| P0   | Plex catalog + history + scoring + table output, read-only | Done |
| P0.1 | Pinning (library + title), recency floor, projected-tier footer | Done |
| P1   | Filesystem tier detection, path translation, majority-bytes rollup | Done |
| P0.2 | Collection-aware grouping (Harry Potter, etc.) | Pending |
| P2   | `--apply`: rsync moves + Plex rescan | Pending |
| P3   | Hardening: lock file, currently-playing skip, free-space check, move cap | Pending |
| P4   | Scheduled cron + size-triggered wrapper | Pending |

P0.2 slots before P2 in priority because it's a scoring-layer concern, not
a moves-layer concern.

## Non-obvious design decisions

Anything listed here has a specific failure mode behind it — don't revert
it without reading why.

### Watch history: `plex.history()`, not `viewCount`

`movie.viewCount` / `episode.viewCount` reflect only the **token owner's**
plays, and only update on near-complete watches. Plex Home users and
partial scrubs are invisible to those fields. The first P0 cut used
`viewCount` and was wildly wrong.

Replacement: `plex.history()` which returns playback events across ALL
accounts and includes partial plays.

### `plex.history()` is called **per-section**, not globally

The global `/status/sessions/history/all` endpoint silently truncates on
busy servers. When we first switched from `viewCount` to `history()`,
TV shows with large counts (hundreds) still came back
with zero events because the global response was capped. Some movies
came back fine because it was near the top of the recency list.

Fix in `build_history_index()`: iterate `plex.library.sections()` and
call `plex.history(librarySectionID=section.key)` for each. Scoping per
library keeps the response bounded and plexapi's pagination works.

Global fallback sweep runs only if every per-section call returned zero.

### Per-show history fallback for rematched shows

After a Plex rematch or library move, history events can reference a
stale `grandparentRatingKey` that no longer equals the current
`show.ratingKey`. The section-level sweep then misses those events.

Fix: `_show_history_fallback(show)` calls `show.history()` which asks
Plex for history scoped to *this show's* ratingKey, bypassing the stale
aggregation. Wired as the second-tier fallback in `collect_series()`
before the final `viewCount` safety net.

### Movies deduped by `guid`

Unmerged multi-version movies (4K Extended + 4K + 1080p uploaded
separately) share the same `guid` (e.g. `plex://movie/<tmdb-id>`). Score
once per guid, sum sizes across all versions. `collect_movies()` groups
by guid before scoring.

This is distinct from duplicates across *different libraries* (e.g. the
same movie in "Movies" and "Blu-Ray & UHD Movies") — those are kept
separate since they're intentional per-library entries.

### Title-year cosmetic strip

Some Plex agents bake `(YYYY)` into the title. Combined with tier.py's
own `{title} ({year})` rendering you get `"The Grand Tour (2016) (2016)"`.
`_clean_title()` strips a trailing `(YYYY)` from the title if it matches
the `year` field. Cosmetic only — doesn't affect scoring.

### Override precedence

`_compute_recommendation()` applies rules in this exact order:

1. Library pin (case-insensitive exact match on library name) → HOT + pinned
2. Title pin (case-insensitive substring) → HOT + pinned
3. Raw score → HOT / WARM / NEUTRAL
4. Recency floor (only if raw rec was NEUTRAL or WARM, and last_played is
   within `hot_recency_days`) → HOT

Pinning wins over everything. Recency floor only *promotes*, never demotes
— this is important for sanity: pinning never gets overridden by score
decay, but recency can't drag a pinned item into WARM either.

### Tier rollup: majority of bytes

Multi-part items (series with dozens of episodes, movies with multiple
versions) can straddle tiers mid-migration. `resolve_item_current_tier()`
sums bytes per tier and picks the majority (>50%):

- >50% HOT bytes → `HOT`
- >50% WARM bytes → `WARM`
- >50% UNKNOWN bytes → `UNKNOWN`
- 50/50 tie → `MIXED`

`MIXED` items become `MIXED_NEUTRAL` only if score is also NEUTRAL. If
score gives a direction, `MIXED` + HOT rec → `TO_HOT` (finish promotion),
`MIXED` + WARM rec → `TO_WARM` (finish demotion).

### Outcome matrix

See `_combine_outcome()` for the full logic. Key invariants:

- If `current_tier == "UNKNOWN"` (tier detection disabled or path
  translation missing), outcomes degrade to `SHOULD_BE_*` / `NEUTRAL`.
  **Do not change this**: it's how the script stays useful on a
  still-bare-P0 config.
- `PIN_HOT` dominates for reporting. Pinned-but-already-HOT is still
  `PIN_HOT`, not `STAY_HOT`, so operators can spot what's exempt from
  score-based demotion.
- The projected-tier bucket for each outcome is fixed in `_HOT_OUTCOMES`
  / `_WARM_OUTCOMES`. P2 reads this mapping too — if you add a new
  outcome, update the sets.

### Graceful degradation

Tier detection activates only when BOTH `paths.hot_pool_mount` is set AND
at least one array disk is known. Missing either → `current_tier` stays
`UNKNOWN` for everything → outcomes fall back to `SHOULD_BE_*` as in P0.

This means a user can deploy the P1 image against a legacy P0 config
with zero outcome regression. Don't break this by making tier detection
mandatory.

### Plex path translation

Plex reports file paths **as Plex sees them**. If Plex runs in a
separate container / VM / host, those paths won't resolve on the host
running tier.py. The setup that drove this design: Plex in a TrueNAS VM
on a Proxmox host, tier.py in a container on Unraid. Same physical files,
three views. Provides flexibility in path prefix naming inside differing
containers.

`translate_plex_path()` does longest-prefix-wins replacement via
`paths.plex_path_map`. Empty map = no translation = assumes paths match.

## Config shape

`example.tiering.yaml` is the canonical source. It auto-seeds `/config/
tiering.yaml` on first container run (see `_try_seed_config`). Keep the
example:

- **Sanitised** — no real hostnames, library names, Docker Hub usernames,
  account names, or pool names. Use neutral placeholders like
  `/mnt/hot_pool`, "Movies", "4K Movies".
- **Stable-shaped** — every key tier.py reads should appear in the
  example even if it's empty (`[]` / null), because users hand-merge
  config changes and missing keys lead to silent fallbacks.
- **Commented** — every knob should have a one-line reason and, where
  relevant, an example value as a commented-out sub-entry.

`DEFAULT_CONFIG` in `tier.py` is the source of truth for defaults and
merges over the user config via `_deep_merge()`. Keep the two in sync.

## Development rules

### Read-only at runtime (until P2)

The container bind-mounts media read-only. `tier.py` must NOT write
outside `/config`. If you need to spill state, put it in `/config/
state.json` — the volume is persistent and already mounted.

### No `--apply` without phase-gate

`--apply` exists in the argparser and immediately exits with code 3 in
P0/P1. Do not wire it to actual rsync calls without a corresponding phase
bump. P2 opens this up with safety guards (currently-playing skip,
free-space check, move-size cap, lock file).

### Notifiers must not raise

All notifier paths are wrapped in broad `except Exception` that logs and
swallows. Broken notifications must not kill a scheduled run. If you add
a new notifier, follow the same pattern — the composite notifier's
contract depends on it.

### Never put secrets in image layers

`.dockerignore` excludes `tiering.yaml` and `*.log`. Verify before any
change to `.dockerignore` or `Dockerfile` that the Plex token cannot
leak into a published layer. A leaked token is a published-repo
revocation event, not a quick fix.

### Before committing

```bash
# 1. Compile check
python3 -m py_compile tier.py

# 2. YAML validity
python3 -c "import yaml; yaml.safe_load(open('example.tiering.yaml'))"

# 3. No personal info leaking back in
grep -iE "your|personal|information" tier.py example.tiering.yaml README.md
#    should return nothing
```

When adding non-trivial logic (scoring tweak, new outcome, path handling),
include a small inline test harness that stubs plexapi and exercises the
new code. Examples exist in the git history — pattern is:

```python
import sys, types
pa = types.ModuleType('plexapi')
pas = types.ModuleType('plexapi.server')
pae = types.ModuleType('plexapi.exceptions')
# ... stub classes ...
sys.modules.update({'plexapi': pa, 'plexapi.server': pas, 'plexapi.exceptions': pae})
import tier
# ... exercise functions directly ...
```

### Backwards compatibility

The outcome alphabet is the contract between P1 (this repo) and P2 (the
mover, not yet written). Adding a new outcome is fine but requires:

1. Assigning it to a projected-tier bucket in `_HOT_OUTCOMES` /
   `_WARM_OUTCOMES` (or neither, for NEUTRAL-bucket).
2. Documenting it in the README "Outcomes" table.
3. Thinking about what P2 should do with it (STAY / promote / demote /
   no-op).

Renaming or removing an outcome breaks P2 when it lands. Don't.

## Gotchas that have bitten us

- **ratingKey drift after rematches** — see the per-show history fallback
  section above.
- **TrueNAS-VM Plex reports TrueNAS paths** — `plex_path_map` is
  non-optional for that setup. Without it, everything resolves UNKNOWN
  and P1 silently falls back to P0 outcomes.

## How to onboard a change

1. Read the relevant section above before editing.
2. Read the README for the user-facing contract.
3. Make the change; add a test harness if the logic is non-trivial.
4. Run the three-step pre-commit check.
5. Update `example.tiering.yaml` if the config shape changed, and update
   the README if user-facing behaviour or an outcome changed.
6. Update the phase table in this file and in `tier.py`'s module
   docstring if the change closes out a phase.
