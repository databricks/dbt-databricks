# Eval spec: pr-ready

The equivalence rubric (decision dimensions) and branch-coverage corpus for the
`dbt-databricks-pr-ready` skill. Co-located with the skill so the eval contract travels with what it
measures: a behavioral A/B replays these inputs through the pre-change and post-change skill and
checks the decisions still match (OLD = the skill at a base git ref, NEW = the working tree). Docs
only — no runtime impact.

## dims — decision dimensions (the equivalence rubric)

`the gap set as met/gap/n-a per rubric item (changelog, functional, unit, lint, conflicts, CI); the self-PR hard-stop identity check it performs; whether test-only changelog entry is required; push identity/command`

## corpus — branch-coverage cases

Grounded in real historical PRs. One row per decision branch; no happy-path-only sampling. Decision
arms are token-cheap (text-eval, no cluster); the `liveCase` column drives the live end-to-end layer
of a full run.

| id | branch | input | liveCase (full run) |
|---|---|---|---|
| p1 | mixed gaps | PR https://github.com/databricks/dbt-databricks/pull/1546 | worktree created at the skill's derived base; assessment only, NO push |
| p2 | mixed gaps | PR https://github.com/databricks/dbt-databricks/pull/1566 | — |
| p3 | changelog/test gaps | PR https://github.com/databricks/dbt-databricks/pull/1350 | — (asserts test-only Under-the-Hood entry IS required) |
| p4 | --auto selection | `--auto` https://github.com/databricks/dbt-databricks/pull/1444 | — |

## Coverage audit

Every dim maps to ≥1 case: gap set (changelog/functional/unit/lint/conflicts/CI) → p1, p2, p3;
self-PR hard-stop identity check → p1, p2 (any real PR run performs it); test-only changelog entry
required → p3; push identity/command → p4 (`--auto` exercises the push path). Add a case before
running if a new dim is introduced.
