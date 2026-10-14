# Merge-readiness rubric — dbt-databricks PRs

Assess every item as **met**, **gap**, or **n-a**. Each gap gets a one-line
recommended fix and rough effort estimate.

## 1. Changelog entry

- **Detection:** `CHANGELOG.md` is touched; its entry is under the topmost
  `(TBD)` heading, in the correct section, and uses the repository's current
  link and contributor-credit conventions. A missing entry is a gap for every
  PR, including test-only changes.
- **Recommended:** Always. Test-only changes use an `Under the Hood` entry that
  says `(test-only, no runtime impact)`.
- **Fix recipe:** Add or correct one present-tense, user-visible entry using the
  `CHANGELOG.md` conventions in `AGENTS.md`. Do this last. Commit:
  `chore: update CHANGELOG for #N`.

## 2. Functional tests

- **Detection:** A behavior change under `dbt/adapters/databricks/` or
  `dbt/include/databricks/` lacks a functional test, or the changed test does
  not exercise the new branch.
- **Recommended:** When behavior is observable against a live warehouse.
  Optional for pure refactors, logging, or dependency updates.
- **Fix recipe:** Follow `docs/testing.md`: server-observable assertions only,
  rerun-safe tests, reusable fixtures, and the nearest existing test module.
  For a bugfix, prove the targeted test fails without the fix and passes with
  it whenever the relevant service is available, then run the surrounding
  module. If the service is unavailable, record the concrete blocker. If the
  A/B setup itself is objectively impractical despite an available service,
  record the specific constraint plus a justified substitute verification and
  its evidence in the report; do not silently omit the fail/pass proof. Commit:
  `test: add functional coverage for <x>`.

## 3. Unit tests

- **Detection:** Changed Python logic with branches is not exercised by a unit
  test. Search existing tests for the affected symbols before flagging a gap.
- **Recommended:** For new or changed pure-Python logic. Optional for macro or
  SQL-only behavior already covered functionally.
- **Fix recipe:** Follow the verify-then-fix loop with the targeted unit test.
  Commit: `test: add unit coverage for <x>`.

## 4. Lint and format

- **Detection:** In the dedicated worktree, `hatch run pre-commit run
  --all-files` fails. In read-only mode, never run this potentially mutating
  hook; use existing public code-quality check evidence and report when that
  evidence is absent.
- **Recommended:** Always.
- **Fix recipe:** Apply necessary formatting or lint fixes and rerun until
  clean. Commit: `style: apply ruff format` or `chore: fix lint`.

## 5. Conflicts and base branch

- **Detection:** `mergeable` is `CONFLICTING`. Separately, note a suspicious
  base branch but never retarget automatically.
- **Recommended:** Conflict resolution is recommended; retargeting is a human
  decision.
- **Fix recipe:** Fetch the stated base branch directly from the upstream
  repository, merge it in the dedicated worktree, resolve conflicts, rerun the
  touched tests, and re-check changelog placement. A merge commit is expected
  here.

## 6. CI status

- **Detection:** Inspect `statusCheckRollup`. Fold a code-quality failure into
  lint and a PR-caused integration failure into the relevant test item.
- **Recommended:** Note-only unless it maps to another rubric item. Classify a
  failure as infrastructure only when public check evidence shows a cause
  unrelated to the diff or an unchanged rerun passes. Otherwise keep it
  unresolved; do not infer that a failure is transient.

## Ignore as contributor noise

- Existing merge commits do not count against the contributor.
- Lint debt outside the PR's blast radius is noted as pre-existing even when it
  must be repaired for CI.
- Documentation follow-up outside this repository is not a rubric item.
