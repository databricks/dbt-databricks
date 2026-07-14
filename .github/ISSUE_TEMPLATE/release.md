---
name: Release
about: Release a new version of dbt-databricks
title: ''
labels: release
assignees: ''

---

### TBD

### Pre-release checks

- [ ] Check that `.github/test_timings.json` is not stale. The weekly
      `Refresh Test Timings` workflow
      (`.github/workflows/refresh-test-timings.yml`) opens a PR when shard
      balance has drifted meaningfully; make sure any open refresh PR has been
      reviewed and merged. To refresh manually, run
      `python scripts/regenerate_timings.py`. Stale timings let the integration
      shards drift out of balance and inflate the matrix wall-clock.
