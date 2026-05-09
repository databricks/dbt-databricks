import pytest
from dbt.adapters.base import BaseRelation
from dbt.tests import util

from dbt.adapters.databricks.relation import DatabricksRelationType

SEED_CSV = """id,value
1,100
2,200
""".lstrip()

MV_WITH_TAGS = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
    databricks_tags={'deployment': 'DBT'},
) }}
select * from {{ ref('mv_seed') }}
"""


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewRefreshesWithTags:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mv_seed.csv": SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"mv_with_tags.sql": MV_WITH_TAGS}

    @staticmethod
    def _row_count(project, relation: BaseRelation) -> int:
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    def test_mv_refreshes_on_rerun_when_tags_set(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", "mv_with_tags"])

        mv = project.adapter.Relation.create(
            identifier="mv_with_tags",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.MaterializedView,
        )
        seed = project.adapter.Relation.create(
            identifier="mv_seed",
            schema=project.test_schema,
            database=project.database,
        )

        assert self._row_count(project, mv) == 2

        project.run_sql(f"insert into {seed} values (3, 300)")
        assert self._row_count(project, mv) == 2

        util.run_dbt(["run", "--models", "mv_with_tags"])

        # get_relation_config goes through DeltaLiveTableAPIBase.get_from_relation,
        # which polls the DLT pipeline until any in-flight refresh completes.
        with util.get_connection(project.adapter):
            project.adapter.get_relation_config(mv)

        assert self._row_count(project, mv) == 3, "MV not refreshed on rerun."
