---
name: dbt-databricks-pr-ready
description: Use when given a community dbt-databricks PR (link or number) to prepare for merge — assessing what it lacks (changelog, tests, lint, conflicts) and implementing the missing pieces on the contributor's branch. Args: <pr-link-or-number> [--auto] (--auto skips the selection question and pushes automatically; for offline/CI/goal runs).
---

# dbt-databricks PR merge-readiness driver

Input: a PR URL or bare number for `databricks/dbt-databricks`, optional
`--auto`. The deliverable is verified commits on the contributor's branch plus
a report. Pushing those commits is **approval-gated**: in interactive mode wait
for explicit user approval before pushing; under `--auto` the flag is itself the
authorization and the push runs automatically (see Phase 5).

Hard rules (apply in both modes, no exceptions):

- Never merge, comment on, or label the PR.
- Never push without authorization. Interactive mode: push only after explicit
  user sign-off on the commits. `--auto`: the flag is the authorization — push
  automatically. Either way, re-verify the head hasn't moved immediately before
  pushing (Phase 5).
- Only verified work gets committed (tests run and read, lint clean).
- No attribution footers on commits.
- If the PR head moves mid-run (contributor pushed), stop and report —
  never rebase silently.

## Phase 1 — Setup

Derive the primary checkout once, reused throughout (including later phases):
`PRIMARY="$(git worktree list --porcelain | head -1 | sed 's/^worktree //')"`

1. Ensure `gh` is authed to the account you use for `databricks/dbt-databricks`,
   and capture your login once: `ME=$(gh api user -q .login)`. Fetch metadata:
   `gh pr view <N> --repo databricks/dbt-databricks --json number,title,author,state,isDraft,baseRefName,headRefName,headRepositoryOwner,mergeable,statusCheckRollup,closingIssuesReferences,files,maintainerCanModify`
   Record the head SHA (`gh pr view <N> --json headRefOid`) — used for the
   moved-head check before committing. `mergeable` is computed lazily and
   often comes back `UNKNOWN` on the first fetch — re-query it after a few
   seconds before treating it as a verdict.
2. Hard stops: state is MERGED or CLOSED; or the PR author is you (`$ME`) —
   this tool is for preparing *other* contributors' PRs. Draft PR:
   ask the user in interactive mode; proceed under `--auto`.
3. Locate or create the working worktree `$WT`. A branch can be checked out in
   only one worktree, so `gh pr checkout` fails with "already used by worktree
   at …" when the branch is already checked out elsewhere — common for your
   own / in-repo PRs. **Check for an existing worktree on the branch before
   creating one.** A prior run may have checked it out under `<headRefName>`
   (non-fork PRs) or, for a fork PR, `<author>-<headRefName>` (`<author>` =
   `headRepositoryOwner.login` from step 1). Match either:

   `WT="$(git -C "$PRIMARY" worktree list --porcelain | awk -v r1="branch refs/heads/<headRefName>" -v r2="branch refs/heads/<author>-<headRefName>" '/^worktree /{w=$2} $0==r1||$0==r2{print w}')"`

   - **`$WT` non-empty — reuse it** (do NOT create `pr-<N>` or run
     `gh pr checkout`; both would fail). `cd "$WT"`, then re-sync to the current
     PR head, fetching first so the compare is against the live server ref:
     `UP="$(git -C "$WT" rev-parse --abbrev-ref '@{upstream}' 2>/dev/null || echo origin/<headRefName>)"`;
     `git -C "$WT" fetch "${UP%%/*}" <headRefName>` (refreshes `$UP`);
     `git -C "$WT" log "$UP"..HEAD --oneline` — any commits are prior-run local
     work, so report and keep them (never discard); then
     `git -C "$WT" merge --ff-only "$UP"` (a non-fast-forward failure means the
     local branch diverges from the head — surface and stop, per the
     never-rebase rule). Note in the report that an existing worktree was
     reused instead of `pr-<N>`.
   - **Else if `$PRIMARY/.claude/worktrees/pr-<N>` already exists:**
     `WT="$PRIMARY/.claude/worktrees/pr-<N>"`; `cd "$WT" && gh pr checkout <N>`
     (re-syncs to the current head), first surfacing leftover local commits
     (`git log <remote-tracking-branch>..HEAD`) instead of discarding.
   - **Else create one:** `WT="$PRIMARY/.claude/worktrees/pr-<N>"`;
     `git -C "$PRIMARY" worktree add "$WT" --detach`, then
     `cd "$WT" && gh pr checkout <N>` (sets up the fork remote and a tracking
     branch, so the final push command is exact).

   `$WT` is now the working worktree. Record its push remote once (used in
   Phase 5): `REMOTE="$(git -C "$WT" rev-parse --abbrev-ref '@{upstream}' | sed 's#/.*##')"`
   — `origin` for in-repo PRs, the fork remote `gh pr checkout` set up otherwise.
4. Per-worktree setup (one time) — `.claude` granular symlinks:
   `cd "$WT" && mkdir -p .claude && cd .claude && for item in pr-ready settings.local.json; do ln -sf "$PRIMARY/.claude/$item" "$item"; done`
   (mirror any other shared `.claude/` items your worktree convention calls
   for; never `scheduled_tasks.json` / `worktrees/`).
   Skip `pre-commit install` if `core.hooksPath` is set (the install refuses
   when it is); lint is enforced by the explicit
   `hatch run pre-commit run --all-files` runs in the rubric and Phase 5
   regardless.

Sandbox note: if worktree creation is blocked by a Bash sandbox, disable the
sandbox for the worktree-creation / checkout / symlink commands (as for
git/gh commands generally).

## Phase 2 — Assessment

Read `references/rubric.md` (in this skill directory) and walk every item
against the PR diff and metadata. Produce the gap report:

- one row per rubric item: **met / gap / n-a**, with evidence
- every gap: one-line recommended fix, rough effort, and whether the rubric
  marks it *recommended* or *optional* for this PR

Write the report skeleton to
`$PRIMARY/.claude/pr-ready/pr-<N>/report.md` now (it is
updated as phases complete). If there are **no gaps**, finish the report as
"merge-ready as-is" and stop.

## Phase 3 — Selection

- **Interactive (default):** present gaps via AskUserQuestion, multiSelect,
  one option per gap — label = rubric item, description = recommended fix +
  effort. Recommended items first. More than 4 gaps → split across multiple
  questions in the same call.
- **`--auto`:** select every gap marked *recommended*; skip *optional* gaps
  and record them in the report as skipped-by-policy. Record the selection
  set and selector ("user" or "auto") in the report.

## Phase 4 — Execution

Work the selected items as an explicit checklist (TaskCreate one task per
item) — completion criteria per item come from the rubric's fix recipe.
Ordering: code-touching items first, lint second-to-last, changelog last.

For each item:

1. Follow the rubric fix recipe, including its verify-then-fix loop. Run the
   tests and read the output — never assume.
2. Before committing, confirm the PR head hasn't moved
   (`gh pr view <N> --json headRefOid` equals the recorded SHA).
3. Commit just that item's changes with the rubric's commit message style.
4. Item can't complete after 3 fix attempts (or cluster unavailable): mark it
   **blocked** in the report, stash or leave the partial diff uncommitted,
   and move on — a blocked item never blocks the others.

## Phase 5 — Landing & report

1. `cd "$WT" && hatch run pre-commit run --all-files` — must be clean.
2. Simplify pass over the skill's own additions only (not the contributor's
   code).
3. Re-run the full set of tests touched in Phase 4; read the output.
4. Finalize `$PRIMARY/.claude/pr-ready/pr-<N>/report.md`:
   assessment table, selection (and selector), per-item outcome with test
   evidence, blocked items, noise notes (pre-existing lint, base-branch
   note).
5. Land the commits — **push is approval-gated**:
   - **Interactive (default):** output the per-item outcome table and the exact
     push command (`cd "$WT" && git push "$REMOTE" HEAD:<headRefName>`, `$REMOTE`
     from Phase 1), then ask for explicit approval. Push it
     yourself only on a clear "yes"; otherwise stop and leave the command for the
     user.
   - **`--auto`:** push automatically (the flag is the authorization). Report the
     result.
   - **Before any push, either mode:** re-confirm `gh` is authed to that same
     account, then re-confirm the remote PR head still equals the recorded SHA
     (moved-head check) — if it moved, do **not** push; stop and report. The
     push uses the Phase-1 `$REMOTE` (`git push "$REMOTE" HEAD:<headRefName>`),
     never a personal push alias that targets your own fork namespace. After
     pushing, verify the remote head advanced to your commit and record it in
     the report.
