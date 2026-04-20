import pytest
from dbt.tests.util import check_relations_equal, run_dbt

from tests.functional.adapter.fixtures import MaterializationV2Mixin

_MODEL_SQL = """
{{ config(
    materialized='incremental',
    unique_key='id',
    skip_merge_on_empty_source=true,
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

-- Delta filter: only rows with id greater than existing max (=> empty on 2nd run)
select cast(id as bigint) as id, msg from (
  select 1 as id, 'hello' as msg
  union all
  select 2 as id, 'goodbye' as msg
) src
where id > (select max(id) from {{ this }})

{% endif %}
"""

_SEED_AFTER_FIRST_RUN = """id,msg
1,hello
2,goodbye
"""


class TestSkipMergeOnEmptySource:
    @pytest.fixture(scope="class")
    def models(self):
        return {"skip_merge_model.sql": _MODEL_SQL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": _SEED_AFTER_FIRST_RUN}

    def test_skip_merge_when_source_empty(self, project):
        # 1st run: seeds target with 2 rows
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1

        # 2nd run: incremental with empty delta -> short-circuit should trigger
        results = run_dbt(["run"])
        assert len(results) == 1
        # Data must be unchanged (no MERGE happened, table same as after 1st run)
        check_relations_equal(project.adapter, ["skip_merge_model", "expected"])


class TestSkipMergeOnEmptySourceV2(MaterializationV2Mixin, TestSkipMergeOnEmptySource):
    """Same behavior under V2 materialization path."""


class TestSkipMergeDefaultDisabled:
    """When `skip_merge_on_empty_source` is not set, behavior is unchanged
    (MERGE runs as before, even if source is empty)."""

    @pytest.fixture(scope="class")
    def models(self):
        # Same model but WITHOUT the skip flag
        return {"default_model.sql": _MODEL_SQL.replace("skip_merge_on_empty_source=true,", "")}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": _SEED_AFTER_FIRST_RUN}

    def test_default_no_skip(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        # 2nd run without the flag still succeeds (MERGE with empty source)
        results = run_dbt(["run"])
        assert len(results) == 1
        check_relations_equal(project.adapter, ["default_model", "expected"])
