---
name: dbt-databricks-pr-ready
description: "Use for an open dbt-databricks pull request, including your own PR or a fork PR, to assess merge readiness and optionally repair selected gaps on the PR head branch. Accepts a PR link or number and optional --auto."
---

# dbt-databricks PR-ready compatibility entry point

Derive the active repository root first with
`ROOT="$(git rev-parse --show-toplevel)"`. Then read and follow the canonical
skill at `$ROOT/.agents/skills/dbt-databricks-pr-ready/SKILL.md`. That file is
the only source of the workflow and its rubric.
