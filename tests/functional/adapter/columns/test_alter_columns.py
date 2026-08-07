import pytest
from dbt.tests import util

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.functional.adapter.columns import fixtures
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class SyncAllColumnsWidensType:
    @pytest.fixture(scope="class")
    def models(self):
        return {"type_widen_model.sql": fixtures.type_widening_model}

    def test_sync_all_columns_widens_column_type(self, project):
        relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="type_widen_model",
            type=DatabricksRelation.Table,
        )

        def measure_dtype():
            with project.adapter.connection_named("_test"):
                columns = project.adapter.get_columns_in_relation(relation)
            return {c.column: c.dtype for c in columns}["measure"]

        util.run_dbt(["run", "--full-refresh"])
        before = measure_dtype()

        util.run_dbt(["run"])
        after = measure_dtype()

        assert before != after

        widened = project.run_sql("select measure from type_widen_model where id = 2", fetch="all")
        assert widened[0][0] == 3000000000


class TestSyncAllColumnsWidensType(SyncAllColumnsWidensType):
    pass


class TestSyncAllColumnsWidensTypeV2(MaterializationV2Mixin, SyncAllColumnsWidensType):
    pass
