import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates


models__databricks_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- merge will not happen on the above record where id = 2, so new record will fall to insert
select cast(1 as bigint) as id, 'hey' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""


class TestIncrementalPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_predicates": ["dbt_internal_dest.id != 2"]}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                models__databricks_incremental_predicates_sql
            )
        }


class TestPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["dbt_internal_dest.id != 2"]}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                models__databricks_incremental_predicates_sql
            )
        }
