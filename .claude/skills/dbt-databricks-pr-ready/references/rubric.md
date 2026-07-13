# Merge-Readiness Rubric — dbt-databricks community PRs

Walk every item against the PR diff and metadata. Each item yields one verdict:
**met**, **gap**, or **n-a**. Every gap gets a one-line recommended fix and a
rough effort estimate — these become the selection-question payload.

Sources of truth: `CHANGELOG.md` conventions in the repo, and maintainer
follow-up commits mined from 25 merged community PRs (2026-06-12; e.g. #1472:
functional coverage + ruff format; #1471: changelog entry; #1339: changelog
placement fixes + edge-case test coverage; #1194: changelog author credit).

## 1. Changelog entry

- **Detection:** Diff touches `CHANGELOG.md`; the entry sits under the topmost
  version heading (the one marked `(TBD)`); it is in the correct section
  (`### Features` / `### Fixes` / `### Under the Hood`); it ends with
  `([#N](https://github.com/databricks/dbt-databricks/pull/N))`; when the PR
  addresses a tracked issue, the entry also carries — inside the same parens —
  `resolves [#M](https://github.com/databricks/dbt-databricks/issues/M)`
  (or `partially resolves [#M](…)` when it only partly addresses the issue);
  community contributions credit the author with `(thanks @author!)` before
  the links (repo convention). Older entries use `closes`; leave them, use
  `resolves` for new ones. Any of these missing or wrong → **gap**.
  After any merge of main/release-branch into the PR, re-verify placement —
  bad merges have shifted entries into already-released sections (#1339).
- **Recommended:** always — even test-only changes get an `Under the Hood`
  entry with a "(test-only, no runtime impact)" note, per repo history.
- **Fix recipe:** Add or correct the entry using the canonical format
  documented in the repo's `AGENTS.md` (CHANGELOG Entries section): one line,
  present-tense summary of the user-visible effect (not the implementation),
  correct section, `(thanks @author!)` for community PRs, PR link, and a
  `resolves` / `partially resolves` issue link when applicable. Do this
  **last** (content may depend on tests added by other items).
  Commit: `chore: update CHANGELOG for #N`.

## 2. Functional tests

- **Detection:** The diff changes behavior — files under
  `dbt/adapters/databricks/` or macros under `dbt/include/databricks/` — but
  has no corresponding `tests/functional/` change, **or** the functional
  change doesn't exercise the new branch (e.g. a new config value never set
  in any fixture). Search `tests/functional/adapter/` for the feature area
  touched; name the nearest module to extend in the gap description.
- **Recommended:** when the change is observable against a live warehouse
  (almost all fixes and features). **Optional** for pure refactors, logging,
  or dependency bumps.
- **Fix recipe:** Follow the repo's `docs/testing.md` (§Functional Tests) —
  server-observable assertions only, no log-substring or generated-SQL text;
  rerun-safe; fixtures in the section's `fixtures.py`; prefer dbt-labs base
  classes / section mixins. Extend the nearest existing test module rather
  than creating a new tree. Verify-then-fix:
  1. Write the test; run `cd <worktree> && hatch run pytest <test path> -v`
     against the live cluster (env vars pre-configured; targeted runs may need
     `--profile databricks_uc_sql_endpoint`). See it pass.
  2. For bugfix PRs, prove the test pins the fix: `git stash push -- dbt/`,
     re-run the test, see it **fail**, `git stash pop`, re-run, see it pass.
  3. If the feature is UC- or warehouse-only, add the repo's standard
     profile-skip decorator (grep `tests/functional` for `skip_profile`) so
     cluster profiles skip rather than fail (#1296 pattern).
  4. Run the surrounding test module for regressions.
  Commit: `test: add functional coverage for <x>`.

## 3. Unit tests

- **Detection:** Python logic changes (config parsing, API clients, utils,
  anything with branches) in `dbt/` whose changed functions are not exercised
  by `tests/unit/` — grep test files for the changed symbols before flagging.
- **Recommended:** for new or changed pure-Python logic. **Optional** when the
  behavior is macro/SQL-only and functional tests cover it.
- **Fix recipe:** Same verify-then-fix loop with
  `cd <worktree> && hatch run unit <test path> -v`.
  Commit: `test: add unit coverage for <x>`.

## 4. Lint / format

- **Detection:** `cd <worktree> && hatch run pre-commit run --all-files`
  reports any failure. (Use `--all-files`, not `--files` — mirrors CI.)
- **Recommended:** always.
- **Fix recipe:** Apply hook auto-fixes, hand-fix the rest, re-run until
  clean. Commit: `style: apply ruff format` or `chore: fix lint`.

## 5. Conflicts / correct base branch

- **Detection:** `mergeable` from `gh pr view` is `CONFLICTING` → **gap**.
  Separately, note (never auto-fix) when the base branch looks wrong for the
  release-branch-first flow — features usually land on the active `*.latest`
  branch (#1339/#1340 targeted `1.12.latest`), fixes often on `main`.
  Retargeting is a human decision; surface it as a note in the report, not a
  selectable item.
- **Recommended:** conflict resolution is recommended; base retarget is
  note-only.
- **Fix recipe:** `git merge origin/<base>` in the worktree, resolve, re-run
  the touched test set, then re-verify changelog placement (item 1). Merge
  commits are expected here (the one exception to per-item single commits).

## 6. CI status

- **Detection:** `statusCheckRollup` from `gh pr view`. Red Code Quality job →
  fold into item 4. Red integration tests → check whether the failure is
  caused by the PR (then it belongs to items 2/3) or is the known transient
  infra signature (INTERNAL_ERROR + MaxRetryDurationError across workers);
  report accordingly.
- **Recommended:** note-only unless it maps onto another item.

## Noise to ignore

- `Merge branch 'main' into ...` commits — never count these against the
  contributor or replicate them except via item 5.
- Pre-existing lint debt outside the PR's blast radius that `--all-files`
  surfaces: still fix it (cheap, CI requires it), but attribute it as
  pre-existing in the report.
- Docs follow-ups (docs.getdbt.com) are handled by the separate docs-sync
  workflow — not a rubric item.
