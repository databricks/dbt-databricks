import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)

from tests.functional.adapter.incremental import fixtures


class TestIncrementalPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                fixtures.models__databricks_incremental_predicates_sql
            )
        }


class TestIncrementalPredicatesMergeDatabricksV2(TestIncrementalPredicatesMergeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            },
        }


class TestPredicatesMergeDatabricks(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": (
                fixtures.models__databricks_incremental_predicates_sql
            )
        }


class TestPredicatesMergeDatabricksV2(TestPredicatesMergeDatabricks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+target_alias": "dbt_internal_dest",
            },
        }
