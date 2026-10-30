# Changelog

All notable changes to this project are documented here.

## Unreleased

### Added

- Optional warm-disk allow-list (`warm.allowed_disks`). When set, TO_WARM and
  RELOCATE_WARM destinations, warm per-disk capacity and disk eviction only
  consider the listed array disks; reads (tier detection, TO_HOT) are
  unchanged. Invalid entries (empty list, non-`diskN` name, unmounted disk)
  are a config error at startup.
- New outcome `WARM_ON_DISALLOWED_DISK`: warm media found on a disk outside the
  allow-list is reported with the disk name and not moved. Set
  `warm.relocate_from_disallowed: true` to plan it as RELOCATE_WARM onto an
  allowed disk instead.

Unset allow-list = previous behaviour.
