# Agent guide — plex-media-tiering

Read this before touching the code. It captures decisions that are easy to
re-litigate (and get wrong) from reading the diff alone.

## What this repo is

A single-file Python script (`tier.py`) that asks Plex which media is
hot/cold, probes the local filesystem to see where each item currently
lives, and recommends — and now executes — moves between a fast pool (HOT)
and a parity array (WARM). Move execution is opt-in (`moves.enabled: true`
+ `--apply`); dry-run is the default.

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
| P0.2 | Auto-inherit — when ≥N members of a collection naturally score HOT, promote the whole collection | Done |
| P0.3 | Collection pinning — force every member of a named Plex collection to HOT | Done |
| P0.4 | Added-date floor — promote recently-added movies and TV shows with fresh episodes to HOT | Done |
| P0.5 | Disk eviction mode — mark warm-tier array disks as evicting; items on them get RELOCATE_WARM. Data model + reporting only; actual moves are P2 | Done |
| P1   | Filesystem tier detection, path translation, majority-bytes rollup | Done |
| P2.1 | Move executor — TO_HOT direction; rsync + size-verify + source delete; parity guard; dry-run by default | Done |
| P2.2 | TO_WARM + RELOCATE_WARM moves | Pending |
| P2.3 | Plex rescan automation post-move | Pending |
| P3   | Hardening: lock file, currently-playing skip, free-space check, move cap | Pending |
| P4   | Scheduled cron + size-triggered wrapper | Pending |

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
3. Collection pin (if `item.collection_pinned`) → HOT + pinned
4. Auto-inherit collection pin (if `item.auto_inherit_pinned`) → HOT + pinned
5. Added-date floor (if `item.recently_added`) → HOT
6. Raw score → HOT / WARM / NEUTRAL
7. Recency floor (only if raw rec was NEUTRAL or WARM, and last_played is
   within `hot_recency_days`) → HOT

Pinning wins over everything. Both floors are promote-only, never demote.
The added-date floor sits above raw score so NEUTRAL items get promoted
before the score check. Recency floor sits below raw score and only fires
when the score alone didn't reach HOT.

### Collection pin

`pinned_collections` config is a list of `{library, name}` entries. At
startup, `_build_collection_pinned_keys(plex, pinned_collections_cfg)`
resolves each entry to the set of `ratingKey` ints belonging to that
collection (via `section.collections()` → case-insensitive title match →
`col.items()`). The resulting set is then used in `collect_all()` to stamp
`item.collection_pinned = True` on matching items before scoring.

Key invariants:

- Missing library or collection name → WARNING + skip (not abort).
- Empty `pinned_collections` list → no Plex calls at all (fast path).
- `item.rating_key` is populated for both movies and TV shows from
  `primary.ratingKey` / `show.ratingKey` during collection.
- Collection pin always returns `pinned=True`, so `_combine_outcome()`
  maps it to `PIN_HOT` for all current-tier states — consistent with
  library and title pins.
- Footer: `Collection-pin promotions: N items` (only logged when
  `pinned_collections` is non-empty).

### Auto-inherit collection pin

`auto_collection_inherit` is the automatic counterpart to `pinned_collections`.
After natural scoring (pre-floor, pre-pin), `_build_auto_inherit_keys()` scans
all configured Plex libraries for collections, counting how many members have
`score >= score_to_hot`. If that count meets `min_hot_members`, every member is
added to the inherit set and `item.auto_inherit_pinned = True` is stamped on
matching items in `collect_all()`. `_compute_recommendation()` step 4 then
returns PIN_HOT for those items.

Key design decisions:

**Why natural score, not `current_tier`, as the trigger.** `current_tier` is
a tracking artifact — where the file physically sits on disk. Using it as the
trigger would cause feedback loops: a file promoted to HOT by auto-inherit
would then trigger further inheritance in other collections it belongs to, and
demoting it would immediately re-trigger the rule. Natural score reflects the
user's engagement pattern, which is stable.

**Why `min_hot_members: 2` default, not 1.** A single hot member could be
incidental — one popular episode of a show you don't particularly care about,
or a crossover film that happens to score well. Two hot members signals genuine
engagement with the set as a set.

**Three-branch trigger rule.** `_build_auto_inherit_keys()` applies:

```python
if col_size < min_hot:         # skip — can never trigger; avoids counting hot members
elif col_size == min_hot:      required = max(1, ceil(col_size * min_hot_fraction))
else:                          required = min_hot
# trigger if hot_count >= required
```

The `col_size < min_hot` early exit is a real performance win on libraries with
many 1-item "collections" (Plex creates singletons for some agents).

The `col_size == min_hot` fraction branch exists to avoid a degenerate state:
a 2-member collection with `min_hot_members: 2` would require BOTH members to
be naturally hot — but if both are already hot there is nothing to inherit.
With `min_hot_fraction: 0.5` (default), a 2-member collection only needs 1 hot
member (`ceil(2 * 0.5) = 1`). For all collections larger than `min_hot_members`,
the absolute threshold applies unchanged.

**Precedence: `pinned_collections` > `auto_collection_inherit` > added-floor.**
An item in both an explicit pin and a triggered auto-inherit collection will
have `override = "collection pin"` (step 3 fires before step 4). It is counted
in the `Collection-pin promotions` counter, not `Auto-inherit promotions`. This
preserves the semantic that explicit user intent takes precedence over inferred.

**`skip_smart_collections: true` default.** Smart collections are rules-based
(Plex manages them: "Recently Added", "Top Rated", etc.). Auto-triggering on
them would promote everything that happens to be new or popular — defeating
the specificity of the feature. Manual collections are curated by the user and
are the correct signal.

**No Plex calls when disabled.** `enabled: false` causes `_build_auto_inherit_keys`
to return `(set(), 0, 0)` immediately. No `section.collections()` calls are made,
no log lines are emitted.

### Two-floor promotion model

There are two independent promote-only floors, both set in `thresholds:`.
Neither can demote an item. Either can be disabled by setting its value to
`0` or `null`.

**Recency floor** (`hot_recency_days`): keyed off *playback*. If an item
was last watched within the window, it is promoted to HOT. Designed for
infrequent-but-active shows that the play-weighted score would otherwise
demote (one episode every few weeks scores low, but the user is clearly
watching it).

**Added-date floor** (`added_floor_days_movies` / `added_floor_days_tv`):
keyed off *Plex catalog additions*. If a movie was added recently, or if
any episode in a TV show was added recently, the item is promoted to HOT
without requiring any play history. Rationale: Plex surfaces recently-added
media on the home screen for roughly this long, and users are likely to
watch new additions regardless of their release year.

The two floors are mutually independent — an item can qualify for both,
either, or neither. The added-date floor takes precedence over raw score
(step 3 before step 4) so it fires even on NEUTRAL-score items. The
recency floor fires after raw score (step 5) so it only promotes items the
score didn't already put in HOT.

#### TV performance note

The added-date floor for TV must NOT iterate `show.episodes()` per show —
that is thousands of API calls on a large library. Instead, one call per
TV library section:

```python
cutoff = datetime.now(timezone.utc) - timedelta(days=added_floor_days_tv)
recent_eps = section.search(libtype="episode", addedAt__gte=int(cutoff.timestamp()))
recently_active_shows = {int(ep.grandparentRatingKey) for ep in recent_eps}
```

`_build_recently_active_shows()` encapsulates this. The resulting set is
passed into `collect_series()` as `recently_active_shows` and looked up
O(1) per show via `show.ratingKey in recently_active_shows`.

#### plexapi addedAt filter requires int Unix seconds

`section.search(addedAt__gte=...)` and `addedAt__lte=...` expect an **int
Unix timestamp** (seconds since epoch), not a Python `datetime`. Plexapi's
filter evaluation compares the value against the stored string form of the
Unix timestamp — passing a `datetime` raises:

```text
TypeError: '>=' not supported between instances of 'str' and 'datetime.datetime'
```

Always convert on the caller side: `int(cutoff.timestamp())`. This quirk
does not affect the movies floor, which computes its check with plain
`timedelta` arithmetic on `item.added` without ever calling `section.search`.

The regression test `_test_added_floor_tv_search_uses_int_timestamp` in
`--_test` guards against this being reintroduced: it stubs `section.search`,
captures the kwargs, and asserts `addedAt__gte` is an `int`.

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

### Disk eviction (P0.5)

`array_disk_evict` marks specific warm-tier array disks as evicting. Items
whose files majority-reside on an evicting disk and would otherwise `STAY_WARM`
are changed to `RELOCATE_WARM`. Actual moves are P2.

**Why RELOCATE_WARM exists rather than overloading STAY_WARM.** Eviction is
orthogonal to the tier decision. An item on an evicting disk isn't on the wrong
*tier* — it's on the wrong *disk within that tier*. Overloading STAY_WARM
would lose the signal that P2's mover needs to choose a different destination
disk rather than leaving the file alone. A distinct outcome keeps the
semantics clean and lets P2 read the outcome alphabet unambiguously.

**Why TO_HOT items on evict disks don't get a new outcome.** Moving an item
from an evicting WARM disk to the HOT pool resolves the eviction implicitly —
the item vacates the bad disk regardless of which path triggered the move.
Introducing a parallel outcome (e.g. `EVICT_TO_HOT`) would add combinatorial
complexity with no benefit: P2 would take the same action either way.

**Hot-tier eviction is out of scope for v1.** ZFS pool drive eviction is a
different problem — ZFS handles it via `zpool replace`, not via file-level
moves. Adding hot-tier eviction would require destination-pool selection logic
that belongs in a dedicated ZFS-aware subsystem, not in tier.py's simple
prefix-based classifier.

**Disk validation.** `_build_evict_disks()` validates each configured path
against the effective `array_disks` list (post-auto-detect). A path not in
the effective list gets a WARNING and is skipped — it's not a real warm disk
tier.py knows about, so targeting it would be unsafe.

**Footer accounting.** `RELOCATE_WARM` is in `_WARM_OUTCOMES` so the
projected-WARM size total includes items that will relocate within the warm
tier. They are NOT leaving the warm tier, just changing disks within it.

**Dominant warm disk rollup (`Item.current_disk`).** TV series commonly
span multiple array disks (older seasons on one, newer on another).
The eviction pass needs to attribute a series to exactly one disk. The
chosen rule is **majority bytes**: whichever array disk holds the most
bytes of the item wins and is stored as `Item.current_disk`. Consequences:

- An item with only a small minority of bytes on an evicting disk does
  **not** trigger `RELOCATE_WARM` — if 90% of a series is on disk1 and
  10% on disk7, evicting disk7 does not justify moving the whole series.
- `Item.current_disk` is only populated when `current_tier == "WARM"`.
  HOT items don't get one — there is no per-disk concept on the ZFS pool.
- Do not refactor to "first disk found" or a list of all disks containing
  the item. The majority-bytes winner is intentional.

### Graceful degradation

Tier detection activates only when BOTH `paths.hot_pool_mount` is set AND
at least one array disk is known. Missing either → `current_tier` stays
`UNKNOWN` for everything → outcomes fall back to `SHOULD_BE_*` as in P0.

This means a user can deploy the P1 image against a legacy P0 config
with zero outcome regression. Don't break this by making tier detection
mandatory.

### Plex path translation + Unraid user-share resolution

Plex reports file paths **as Plex sees them**. If Plex runs in a
separate container / VM / host, those paths won't resolve on the host
running tier.py. The setup that drove this design: Plex in a TrueNAS VM
on a Proxmox host, tier.py in a container on Unraid. Same physical files,
three views. Provides flexibility in path prefix naming inside differing
containers.

`translate_plex_path()` does longest-prefix-wins replacement via
`paths.plex_path_map`. Empty map = no translation = assumes paths match.

After translation, paths that still start with `paths.user_share_prefix`
(default `/mnt/user`) are handled by a second step: `resolve_user_share()`
probes each candidate tier mount (HOT first, then each WARM disk) for the
file's existence and returns the first path that resolves. This is
necessary on Unraid because Plex is typically given user-share paths
(`/mnt/user/Movies/…`) regardless of which physical disk backs a file —
`classify_path()` only knows real mount prefixes, so without this step
every translated path would return UNKNOWN.

The full per-file pipeline is:

```text
translate_plex_path → resolve_user_share → classify_path
```

`resolve_user_share()` is a no-op when `user_share_prefix` is empty or
the path doesn't start with that prefix, so non-Unraid setups are
unaffected. If a file doesn't exist on any probed mount the original
(unresolved) path is returned and `classify_path()` marks it UNKNOWN,
preserving graceful degradation.

### Move executor (P2.1)

**Why TO_HOT is shipped first (lowest-risk direction).**
The source is the parity-protected Unraid array; the destination is a separate
ZFS pool with no redundancy contract with the source. A failed move doesn't
reduce redundancy — the source is parity-protected until we explicitly delete
it post-verification. There is also no destination-disk selection problem (the
ZFS pool is a single target). This makes TO_HOT the cleanest first cut without
importing P3's free-space-budget complexity.

**Why source deletion is gated on size-verify.**
Silent data loss is the worst possible failure mode for a file mover. rsync
exiting 0 doesn't guarantee a complete transfer — disk-full conditions, network
interruptions, or corruption can leave a truncated destination. The size-verify
step (`du -sb` on source and destination, tolerance ≤ 1 KB) is the only safety
gate between a failed rsync and a deleted source. Never bypass it to save time.

**Why moves are serial, not parallel.**
Parallel rsyncs against a parity-protected Unraid array thrash the parity disk
and can cause I/O contention that degrades both array performance and parity
integrity. The throughput gain from parallelism is marginal compared to the risk.
P2.2 will revisit if operators request it, with explicit caveats.

**Why parity check is a blocker, not a warning.**
Unraid's parity recalculation reads every data block and recomputes expected
parity. A concurrent write (which rsync produces) changes a block after parity
has been sampled, leaving the array with a parity mismatch for that stripe.
This is silent unless the next parity check reveals it. Aborting the move pass
before any rsync call is the only safe behaviour. The `parity_check_blocking:
false` escape hatch exists for operators who understand the risk and have a
specific reason to proceed.

**Why `[SKIPPED]` for already-HOT items is an idempotency net.**
If the script is re-run after a partial apply, some TO_HOT items may have
already arrived on the hot pool (because rsync succeeded but the run was
interrupted before the summary logged). Detecting `current_tier == HOT` on a
TO_HOT item and logging SKIPPED instead of re-rsyncing is a cheap idempotency
guard that prevents double-rsyncing. It does not indicate a scoring error.

**`Item.source_dir` — common-ancestor directory of files on dominant disk.**
`resolve_item_current_tier()` now returns a fourth value: the common ancestor
directory of all resolved file paths on the dominant WARM disk. For a single
movie file at `/mnt/disk4/Movies/Foo/foo.mkv` it is the parent directory
`/mnt/disk4/Movies/Foo`. For a TV series spanning multiple season subdirectories
it is the common prefix, e.g. `/mnt/disk4/TV Shows/Show (2001)`. This is what
P2's rsync uses as the source. Do not refactor to "title-derived path" — the
actual filesystem layout is the ground truth, not the Plex title string.

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

### Write only to `/config` at runtime

The container bind-mounts media read-only. `tier.py` must NOT write
outside `/config`. If you need to spill state, put it in `/config/
state.json` — the volume is persistent and already mounted. The one
exception is the move executor, which writes to the hot pool destination;
that path is a separate read-write mount.

### `--apply` is now live in P2.1 (TO_HOT only)

`--apply` executes TO_HOT moves when `moves.enabled: true`. Adding
support for new move directions (TO_WARM, RELOCATE_WARM) requires a
corresponding phase bump to P2.2. The full P3 safety guards (currently-
playing skip, free-space check, move-size cap, lock file) are still
pending — don't add them piecemeal without the full P3 spec.

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

#### Automated in CI

Every push and pull request targeting `main` runs `.github/workflows/test.yml`,
which executes the three checks below automatically. A failing check blocks
PR merge (once branch protection is enabled). You do not need to run these
as a manual ritual, but running them locally first gives a fast-fail loop
before waiting for CI.

#### Run locally before pushing

macOS system `python3` lacks pyyaml and plexapi. **Always use the venv**
for checks 2 and 3; check 1 (compile) is fine with bare python3.

```bash
# Bootstrap venv once (skip if /tmp/pmt-venv already exists)
python3 -m venv /tmp/pmt-venv && /tmp/pmt-venv/bin/pip install pyyaml plexapi -q

# 1. Compile check (bare python3 is fine — no third-party imports)
python3 -m py_compile tier.py

# 2. YAML validity (needs pyyaml — use venv)
/tmp/pmt-venv/bin/python3 -c "import yaml; yaml.safe_load(open('example.tiering.yaml'))"

# 3. Inline test harness (needs pyyaml + plexapi — use venv)
/tmp/pmt-venv/bin/python3 tier.py --_test
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
