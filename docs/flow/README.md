# Materialization Flow Docs

_Last updated: 2026-08-09_

These docs map how each dbt-databricks materialization executes — the decision branches, the
order of operations, and where shared logic (like relation replacement) is reused. They are
maintained by hand as Mermaid diagrams; **when the diagrams and the macros disagree, the macros
are the source of truth** (see `dbt/include/databricks/macros/materializations/`).

## The `use_materialization_v2` behavior flag

Several materializations ship **two** execution paths, selected at run time by the
`use_materialization_v2` [behavior flag](../dbr-capability-system.md) (defined as
`USE_MATERIALIZATION_V2` in `dbt/adapters/databricks/impl.py`). The flag **defaults to `False`**,
so the "V1" / "Existing" diagram is what most projects run today; the "V2" / "New" diagram is what
runs once a project opts in.

V2 separates *create* from *insert* — it builds an intermediate materialization, then stages and
swaps — which enables more performant column comments and additional column features. Macros branch
on it via `adapter.get_behavior_flag_no_warn('use_materialization_v2')`.

Materializations that honor the flag show both diagrams in their doc:

| Materialization | Flow doc | Honors `use_materialization_v2`? |
| --- | --- | --- |
| Table | [table_flow.md](table_flow.md) | Yes — V1 (default) + V2 |
| View | [view_flow.md](view_flow.md) | Yes — V1 (default) + V2 |
| Incremental | [incremental_flow.md](incremental_flow.md) | Yes — Existing (default) + New |
| Seed | [seed_flow.md](seed_flow.md) | Yes — V1 (default) + V2 |
| Snapshot | [snapshot_flow.md](snapshot_flow.md) | No — single path |
| Streaming table | [streaming_table_flow.md](streaming_table_flow.md) | No — single path |
| Materialized view | [materialized_view_flow.md](materialized_view_flow.md) | No — single path |
| _(shared)_ Relation replacement | [replace_flow.md](replace_flow.md) | Used by the V2 table/view/incremental paths |

## Not yet documented

These have macros but no dedicated flow doc yet: **metric view**, **clone**, and **Python models**
as a language variant of table/incremental. Contributions welcome — until then, read the macros
directly (`materializations/metric_view.sql`, `materializations/clone/`, and the `python` language
branches of `table.sql` / `incremental/`).
