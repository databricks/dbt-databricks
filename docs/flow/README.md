# Materialization Flow Docs

_Last updated: 2026-09-30_

These docs map how each dbt-databricks materialization executes — the decision branches, the
order of operations, and where shared logic (like relation replacement) is reused. They are
maintained by hand as Mermaid diagrams; **when the diagrams and the macros disagree, the macros
are the source of truth** (see `dbt/include/databricks/macros/materializations/`).

## The `use_materialization_v2` behavior flag

Several materializations ship **two** execution paths, selected at run time by the
`use_materialization_v2` behavior flag (defined as
[`USE_MATERIALIZATION_V2`](../../dbt/adapters/databricks/impl.py) in the adapter). The flag **defaults to `False`**,
so the "V1" / "Existing" diagram is what most projects run today; the "V2" / "New" diagram is what
runs once a project opts in.

For table and incremental models, V2 separates *create* from *insert*: it builds an intermediate
relation and may stage and swap the target when safer relation operations are enabled. View and
seed V2 use the flag too, but do not follow that staging-table pattern. Macros branch on the flag
via `adapter.get_behavior_flag_no_warn('use_materialization_v2')`.

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
| _(shared)_ Relation replacement | [replace_flow.md](replace_flow.md) | Used by view, materialized-view, streaming-table, and metric-view replacement helpers |

## Hook transaction categories

dbt splits hooks into an inside-transaction category (unset or `transaction: true`) and an
outside-transaction category (`transaction: false`, including `before_begin` / `after_commit`).
Pre-hooks run outside then inside; post-hooks run inside then outside. dbt-databricks overrides
`run_hooks` (`materializations/hooks.sql`) because the adapter never opens a transaction, and the
global macro's literal `commit;` before the outside category fails on Databricks.

The `use_non_transactional_hooks` behavior flag (defined as
[`USE_NON_TRANSACTIONAL_HOOKS`](../../dbt/adapters/databricks/impl.py)) controls the outside
category. It **defaults to `False`**: those hooks are skipped without rendering their SQL, and dbt
emits its behavior-change warning once per invocation, only if a skipped hook was encountered.
When enabled, they run at their outside-transaction position with no `COMMIT`.
Inside-transaction hooks are unaffected. Each diagram's hook steps keep their existing positions, so
materialized views and streaming tables still run the outside category on no-op branches that skip
the inside category.

## Not yet documented

These have macros but no dedicated flow doc yet: **metric view**, **clone**, and **Python models**
as a language variant of table/incremental. Contributions welcome — until then, read the macros
directly (`materializations/metric_view.sql`, `materializations/clone/`, and the `python` language
branches of `table.sql` / `incremental/`).
