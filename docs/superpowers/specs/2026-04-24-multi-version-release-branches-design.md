# Multi-Version Release Branches for dbt-databricks-enhanced

**Date:** 2026-04-24
**Status:** Design approved, ready for implementation planning

## Context

`dbt-databricks-enhanced` is a team-maintained fork of `dbt-databricks`. Until now, this fork has operated with a single `main` branch based on upstream `v1.11.6`, and any enhancements (session mode, Python timeout, etc.) accumulated linearly on top.

The team has identified a need to support **multiple upstream versions in parallel**. Concrete scenarios:
- We adopt upstream `1.11.7` but discover a regression → we want to fall back to `1.11.6.x` while still being able to ship a new patch for `1.11.6.x`.
- Later, when `1.12.0` is released upstream, we want to keep shipping fixes on `1.11.x` while preparing `1.12.x`.

The upstream `dbt-databricks` project solves this with parallel `X.Y.latest` release branches (e.g., `1.11.latest`, `1.12.latest`) alongside an active `main` branch. This fork will mirror that model exactly.

## Goals

1. Ship enhanced releases for multiple upstream versions in parallel.
2. Clearly identify which upstream version any given tag is based on.
3. Make it easy to backport a fix from one release line to another.
4. Keep the branching model familiar to anyone who knows the upstream repo.
5. Minimize divergence from upstream so syncing future releases stays straightforward.

## Non-Goals

- Automating release builds (manual wheel builds are fine for now).
- Publishing to a public package index.
- Supporting more than one consumer of the fork (still a team-internal package).

## Design

### Branch Structure

Mirror upstream exactly:

| Branch          | Purpose                                                                  |
|-----------------|--------------------------------------------------------------------------|
| `main`          | Active development. Next unreleased enhancement lands here first.        |
| `1.11.latest`   | Stable release line for `1.11.x.N` enhanced releases.                    |
| `1.12.latest`   | Created when we sync upstream `1.12.0`. Future.                          |

**Rules:**
- New feature work targets `main`.
- Release lines (`X.Y.latest`) only receive: upstream syncs for their version line, and cherry-picked fixes from `main` or from each other.
- Tags are conceptually owned by release-line branches. A tag's commit must be reachable from the corresponding `X.Y.latest` branch; in practice, tag after the release-line branch has the final release commit.

### Versioning

(Carried over from prior discussion, unchanged.)

- Upstream controls `major.minor.patch` (e.g., `1.11.6`).
- Our enhancements append a fourth segment: `1.11.6.1`, `1.11.6.2`, `1.11.7.1`.
- Each release line resets the fourth segment to `.1` when it picks up a new upstream patch release.

Example timeline:
```
1.11.latest:   v1.11.6.1 → v1.11.6.2 → v1.11.7.1 → v1.11.7.2
1.12.latest:   v1.12.0.1 → v1.12.1.1
```

### Release Workflow

1. Merge PR to `main` (active development).
2. When ready to release on a given line, cherry-pick or merge the change into the appropriate `X.Y.latest` branch.
3. On `X.Y.latest`: bump `dbt/adapters/databricks/__version__.py`, update `CHANGELOG.md`, commit.
4. Tag the commit on `X.Y.latest`: `git tag -a vX.Y.Z.N -m "..."`.
5. Push branch and tag, create GitHub Release.

### Upstream Sync Workflow

When upstream ships `1.11.8`:
1. Fetch upstream, create a `sync/upstream-v1.11.8` branch from current `1.11.latest`.
2. Merge `upstream/1.11.latest` into the sync branch. Resolve conflicts.
3. Open a PR from the sync branch into our `1.11.latest` branch.
4. After merge: bump version to `1.11.8.1`, update `.github/last-upstream-sync-tag`, tag and release.

When upstream ships `1.12.0` (new minor version):
1. Create our `1.12.latest` by branching from current `1.11.latest` (to retain our patches) or from `main` (if our patches are already there) — decided case-by-case.
2. Merge `upstream/1.12.latest` into it.
3. Version becomes `1.12.0.1`.

### Backporting Fixes

When a fix is needed on multiple release lines:
1. Develop + merge to `main`.
2. Cherry-pick to each affected `X.Y.latest` branch.
3. Each line gets its own version bump + tag.

### Migration Plan (Execution Steps)

Execute in this order:

1. **Create `1.11.latest` from current `main`.**
   - Current `main` is the 1.11.6.1 release state.
   - `1.11.latest` starts at the same commit.
   - Tag `v1.11.6.1` at the release commit (reachable from both `main` and `1.11.latest` since they share history at this point).

2. **Prepare `1.11.latest` to receive upstream 1.11.7.**
   - The existing `sync/upstream-v1.11.7` branch contains: upstream 1.11.7 merge + session mode work, but **not** the Python timeout fix from 1.11.6.1.
   - Rebase or merge `sync/upstream-v1.11.7` onto `1.11.latest` so the 1.11.6.1 timeout fix is preserved.
   - Resolve conflicts; ensure version becomes `1.11.7.1`.
   - Update `CHANGELOG.md` with a `1.11.7.1` section listing our patches on top of upstream 1.11.7.
   - Tag `v1.11.7.1`.

3. **Redefine `main`'s role.**
   - `main` remains the default GitHub branch and the target for active development PRs.
   - After the 1.11.7 work lands on `1.11.latest`, merge `1.11.latest` into `main` so `main` reflects the most recent enhancements (standard upstream practice).

4. **Update `.github/workflows/upstream-sync.yml`.**
   - Current workflow assumes syncs go to `main`. Change it to open PRs against the appropriate `X.Y.latest` branch based on the upstream tag being synced.
   - A newly-synced upstream `1.11.8` → PR into `1.11.latest`.
   - A newly-synced upstream `1.12.0` → PR into `1.12.latest` (creating it if missing).

5. **Update `CONTRIBUTING.md`.**
   - Replace the current "Post-Merge Release Checklist" with one that reflects the `X.Y.latest` model.
   - Add a "Branch Model" section describing `main` vs. `X.Y.latest`.
   - Add an "Upstream Sync" section describing how new upstream versions land.
   - Add a "Backporting" section with cherry-pick commands.

6. **Update `.github/last-upstream-sync-tag`.**
   - One file per release line is simpler: rename to `.github/upstream-sync-tags/1.11.latest` with contents `v1.11.7` after the sync.
   - Alternative: keep a single file that always records the most-recently-synced tag; noted in CONTRIBUTING.md.

7. **Push branches and tags, document the transition.**
   - Push `1.11.latest` with `v1.11.6.1` and `v1.11.7.1` tags.
   - Update README.md release table with both versions available.

### Component Boundaries

- **Branch conventions** live in `CONTRIBUTING.md` — the documentation unit.
- **Automation** lives in `.github/workflows/upstream-sync.yml` — the CI unit.
- **Sync state** lives in `.github/upstream-sync-tags/<branch>` (or equivalent) — the metadata unit.
- **Version string** lives in `dbt/adapters/databricks/__version__.py` — the package unit.
- **Changelog** lives in `CHANGELOG.md`, ordered by version within release line sections.

Each unit has one purpose and is independently editable.

## Testing

- Manual validation of the migration: after steps 1–2, check out each of `main`, `1.11.latest`, `v1.11.6.1`, `v1.11.7.1`, run the full unit test suite (`pytest tests/unit/`) and ensure it passes on each.
- Wheel-build smoke test: `uv build` on each release tag should produce a valid wheel with the expected version string.
- Cherry-pick rehearsal: pick a trivial commit from `main` onto `1.11.latest` to verify the backport flow works before the team needs it for real.

## Open Questions / Deferred Items

- **Automated release notes generation:** Currently manual via `CHANGELOG.md`. Could be scripted later but not in scope here.
- **Deprecation policy:** How long do we maintain a release line after a new one ships? Not yet decided; document when needed.
- **Pre-release versions:** If we need `1.11.7.1rc1`, the fourth-segment scheme allows it but we haven't tested packaging tooling behavior. Defer until needed.
