# dbt-databricks docs

_Last updated: 2026-08-09_

Internal documentation for the dbt-databricks adapter, grouped by purpose. For user-facing config
reference, see the [dbt docs](https://docs.getdbt.com/reference/resource-configs/databricks-configs).

> **Docs vs. code:** these docs describe adapter internals and workflows, but **the code is always
> the source of truth.** If a doc disagrees with the macros or Python, trust the code and fix the
> doc.

## Architecture & flow (how the adapter works)

- **[flow/](flow/README.md)** — how each materialization executes (table, view, incremental, seed,
  snapshot, streaming table, materialized view) as Mermaid diagrams, plus the shared relation
  [replace flow](flow/replace_flow.md). Start at [flow/README.md](flow/README.md) for the
  `use_materialization_v2` flag that selects between the V1 and V2 diagrams.
- **[dbr-capability-system.md](dbr-capability-system.md)** — the DBR version-capability system that
  gates version-dependent features.

## Contributor docs (setup & testing)

- **[dbt-databricks-dev.md](dbt-databricks-dev.md)** — development environment setup and workflow.
- **[testing.md](testing.md)** — testing strategy (unit, macro, functional).

## Guides (user-facing how-tos)

These are task-oriented tutorials. They are lower priority than the flow/architecture docs and may
drift; treat them as starting points, not authoritative reference.

- **[guides/uc.md](guides/uc.md)** — using dbt-databricks with Unity Catalog.
- **[guides/databricks-jobs.md](guides/databricks-jobs.md)** — running a dbt project as a Databricks
  job.
- **[guides/workflow-job-submission.md](guides/workflow-job-submission.md)** — submitting Python
  models as long-lived Databricks Workflows.
- **[guides/databricks-copy-into-macro-aws.md](guides/databricks-copy-into-macro-aws.md)** — loading
  S3 data into Delta with the `databricks_copy_into` macro.
