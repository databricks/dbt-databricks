---
name: dbt-databricks-pr-ready
description: "Use for an open dbt-databricks pull request, including your own PR or a fork PR, to assess merge readiness and optionally repair selected gaps on the PR head branch. Accepts a PR link or number and optional --auto."
---

# dbt-databricks PR-readiness driver

Input is a URL or bare number for `databricks/dbt-databricks`, plus optional
`--auto`. If no PR is supplied, run `gh pr view` without `--repo` from the
current branch and use the result only when it resolves exactly one PR in
`databricks/dbt-databricks`; otherwise ask for the URL or number. For an
explicit URL or number, use `gh pr view <input> --repo databricks/dbt-databricks`.
`--auto` selects recommended gaps only. It never authorizes a commit or push.

The deliverable is an assessment report and, only when the caller can write the
PR head branch and approves each gate, verified commits on that branch.

## Hard rules

- Never merge, comment on, label, retarget, or create a PR.
- Never rewrite history, rebase silently, or discard local work.
- Never commit or push without explicit user approval after showing the changes
  and verification evidence. `--auto` does not bypass either approval.
- Run and read every selected test and lint command. Commit only verified work.
- Stop if the PR head moves after the run starts. Never overwrite a contributor's
  newer head.
- Do not put run reports, generated evaluation output, personal paths, accounts,
  or local tooling details into committed files. The co-located `eval-spec.md`
  is a checked-in behavioral contract, not run output.

## Phase 1 — Resolve the PR, write scope, and worktree

1. Set `ROOT` to `git rev-parse --show-toplevel`, `REPO` to
   `databricks/dbt-databricks`, `WT` to `$ROOT/.agents/worktrees/pr-<N>`, and
   `REPORT` to `$ROOT/.agents/pr-ready/pr-<N>/report.md`. All are local
   artifacts except the checked-in skill source.
2. Confirm `gh auth status`, capture `ME` with `gh api user -q .login`, and
   fetch PR metadata including its state, draft status, author, base branch,
   head branch, head SHA, head repository, head repository owner,
   `isCrossRepository`, `maintainerCanModify`, `mergeable`,
   `statusCheckRollup`, `closingIssuesReferences`, and `files`.
   Re-query an initially `UNKNOWN` mergeability result before treating it as a
   verdict. Stop for a closed or merged PR, a missing head repository, or a
   missing head ref. Draft PRs are valid inputs.
3. Record `SOURCE_SHA=headRefOid`. Derive `HEAD_SLUG` from
   `headRepositoryOwner.login` and `headRepository.name`; do not use
   `headRepository.nameWithOwner`. Prefer an existing Git remote whose URL
   normalizes to that exact slug. Otherwise use
   `https://github.com/<HEAD_SLUG>.git` directly without adding a persistent
   remote or guessing from the branch name.
4. Classify branch write access before proposing repairs:

   | PR head | Write mode |
   |---|---|
   | Head repository owner is `ME` | `owner` — the contributor may update their own fork branch. |
   | PR is in `REPO` and the caller has `ADMIN`, `MAINTAIN`, or `WRITE` viewer permission | `repository` — update the in-repository head branch. |
   | Fork PR, `maintainerCanModify` is true, and the caller has upstream `ADMIN`, `MAINTAIN`, or `WRITE` permission | `maintainer` — update that contributor's fork head branch. |
   | Any other case | `read-only` — assess only; do not edit, commit, or push. |

   Obtain upstream viewer permission with `gh repo view "$REPO" --json
   viewerPermission`. The PR author is never a hard stop. `maintainerCanModify`
   controls only the third row; it is not required for an in-repository PR or
   for the contributor's own fork.
5. For `owner`, `repository`, or `maintainer` mode, fetch the exact source ref
   into `refs/pr-ready/<N>/head` and verify it equals `SOURCE_SHA`. Do not use
   `gh pr checkout`: it does not provide a stable, unambiguous push target for
   this workflow.
   - If `WT` does not exist, create a detached worktree at that ref.
   - If it exists, require a clean worktree and require `HEAD` to equal the
     fetched source ref. If it contains prior local commits, report them and
     stop without discarding or including them in this run. If it diverges,
     report the divergence and stop.
   - Do not create symlinks or shared configuration inside the worktree.
   - In `read-only` mode, do not create, check out, edit, or configure `WT`.
     Gather assessment evidence from PR metadata, the PR diff, public check
     output, and other non-mutating reads instead.
6. In a writable mode only, install pre-commit hooks in `WT` when
   `core.hooksPath` is unset. When it is set, record that hooks are configured
   externally. Never install or run pre-commit hooks in `read-only` mode; hooks
   may mutate files. The explicit all-files pre-commit run in Phase 5 remains
   mandatory for writable runs.
7. Create the report skeleton at `REPORT`, noting PR URL, `SOURCE_SHA`, write
   mode, resolved head slug, and whether the dedicated worktree was reused.

## Phase 2 — Assessment

Read `references/rubric.md` and evaluate every item against the PR metadata and
diff. The report must contain one **met**, **gap**, or **n-a** row per item;
each gap needs evidence, a one-line fix, rough effort, and its recommended or
optional status. If there are no gaps, mark the PR merge-ready as-is and stop.

## Phase 3 — Selection

- Interactive mode: present each gap as a selectable option, recommended items
  first, and record the user's selection.
- `--auto`: select every recommended gap and record optional gaps as
  skipped-by-policy.

Selection authorizes edits and verification only. It does not authorize a
commit or push. In `read-only` mode, report the selected fixes but do not start
an edit; explain that the PR head is not writable by the current caller,
finalize the assessment and selection report, and stop immediately. Do not
enter Phase 4 or Phase 5, edit a worktree, or invoke a mutating hook.

## Phase 4 — Repair selected gaps

Work selected items in this order: code and test changes, lint, then changelog.
For every item, follow the rubric's fix recipe and read the resulting test
output. Review the run's additions for unnecessary complexity, show the exact
diff and test evidence, and obtain explicit commit approval. After approval and
immediately before the commit, re-check that the PR head still equals
`SOURCE_SHA`. Commit only that verified item and never add attribution footers.

After three unsuccessful repair attempts or an unavailable required service,
mark the item blocked. If the run created a partial diff, preserve it with
untracked files in a clearly named stash, for example:

```bash
git stash push --include-untracked \
  -m "pr-ready #<N>: blocked <rubric-item> partial repair"
```

Record the stash reference and name in the report, then require
`git status --porcelain` to be empty. If the stash cannot be created or the
worktree cannot be made clean without discarding work, stop the entire run.
Only after successful preservation and cleanup may independent selected items
continue. Apply the same preservation rule before any hard stop that leaves
run-created changes, including a moved-head stop; never apply a blocked-item
stash during final verification.

## Phase 5 — Final verification and push

1. Require `git status --porcelain` to be empty, record
   `VERIFIED_SHA=$(git rev-parse HEAD)`, and confirm the index and worktree have
   no diff from `HEAD`. From `WT`, run
   `hatch run pre-commit run --all-files`; it must pass without changing the
   worktree. If it creates a diff, final verification fails; handle that diff
   as a separately approved repair or preserve it and stop.
2. Re-run every test touched by the selected repairs and read the output.
   Require `HEAD` still to equal `VERIFIED_SHA` and the worktree still to be
   clean afterward. This verification covers committed `HEAD` only; results
   from a leftover partial diff do not count.
3. Finalize `REPORT` with the assessment table, selection method, write mode,
   per-item outcome and evidence, blocked items, and pre-existing noise.
4. If no local commits were created, stop after the report.
5. Before pushing, show the exact destination and ask for explicit approval.
   After approval and immediately before the push:
   - Re-query `CURRENT_ME=$(gh api user -q .login)` and require it to equal the
     captured `ME`.
   - Re-fetch the PR's head SHA, head ref, head repository and owner,
     `isCrossRepository`, and `maintainerCanModify`, plus the upstream
     `viewerPermission`.
   - Recompute the Phase 1 write mode from those fresh values. Require the
     applicable ownership, repository permission, or opted-in maintainer
     condition still to hold, the result still to equal the captured writable
     mode, and the resolved head repository and branch still to match the
     approved destination.
   - Require the fresh remote head SHA to equal `SOURCE_SHA`, local `HEAD` to
     equal `VERIFIED_SHA`, and the worktree to remain clean.

   If any check fails, do not push. Otherwise push only to the resolved head
   repository and branch with the exact-SHA lease:

   ```bash
   git push --force-with-lease="refs/heads/<headRefName>:<SOURCE_SHA>" \
     <resolved-head-remote-or-url> HEAD:refs/heads/<headRefName>
   ```

   If the lease or permission check fails, do not retry with a broader force
   option; leave commits local and report the failure. After a successful push,
   verify that the PR head equals local `HEAD` and record that SHA in `REPORT`.
