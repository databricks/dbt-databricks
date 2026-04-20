import pytest
from dbt.tests import util

from tests.functional.adapter.microbatch.fixtures import (
    microbatch_seeds_csv,
    schema_yml,
)

# Uses replace_where strategy (same macro as microbatch) to verify that
# column reordering between runs does not corrupt data.
_initial_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates="id >= 2",
) }}

{% if not is_incremental() %}
select id, event_time, amount from {{ ref('microbatch_seeds') }}
{% else %}
select id, event_time, amount from {{ ref('microbatch_seeds') }} where id >= 2
{% endif %}
"""

# Same logic but columns in a different order (amount, id, event_time)
_reordered_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates="id >= 2",
) }}

{% if not is_incremental() %}
select amount, id, event_time from {{ ref('microbatch_seeds') }}
{% else %}
select amount, id, event_time from {{ ref('microbatch_seeds') }} where id >= 2
{% endif %}
"""


class TestReplaceWhereColumnOrder:
    """
    Verifies that the replace_where strategy (shared with microbatch) preserves
    data integrity when column order changes between the temp and target tables.

    The fix uses INSERT BY NAME so that columns are matched by name, not position.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "replace_where_col_order.sql": _initial_model_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "microbatch_seeds.csv": microbatch_seeds_csv,
        }

    def test_replace_where_column_order(self, project):
        # Seed and initial run — creates the table with original column order
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        # Verify initial data
        actual_initial = project.run_sql(
            f"select id, amount from {project.database}.{project.test_schema}"
            ".replace_where_col_order order by id",
            fetch="all",
        )
        assert len(actual_initial) == 3
        assert actual_initial[0] == (1, 100)
        assert actual_initial[1] == (2, 200)
        assert actual_initial[2] == (3, 300)

        # Swap to the reordered model (amount, id, event_time)
        util.write_file(
            _reordered_model_sql,
            str(project.project_root) + "/models/replace_where_col_order.sql",
        )

        # Incremental run with reordered columns
        util.run_dbt(["run"])

        # Verify data integrity — columns must match by name, not position
        actual_final = project.run_sql(
            f"select id, amount from {project.database}.{project.test_schema}"
            ".replace_where_col_order order by id",
            fetch="all",
        )

        assert len(actual_final) == 3
        # id=1 was not replaced (predicate is id >= 2), so it stays the same
        assert actual_final[0] == (1, 100)
        # These rows were replaced — if column order was wrong, amount and id
        # would be swapped (e.g., id=200 amount=2 instead of id=2 amount=200)
        assert actual_final[1] == (2, 200)
        assert actual_final[2] == (3, 300)
