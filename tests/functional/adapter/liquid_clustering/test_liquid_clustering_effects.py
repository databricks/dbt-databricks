import pytest
from dbt.tests import util

from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringProcessor
from tests.functional.adapter.fixtures import MaterializationV2Mixin, RerunSafeMixin
from tests.functional.adapter.liquid_clustering import fixtures


def get_tblproperty(project, identifier, key):
    rows = project.run_sql(
        f"show tblproperties {{database}}.{{schema}}.{identifier}",
        fetch="all",
    )
    values = [row[1] for row in rows if row[0] == key]
    return values[0] if values else None


def get_clustering_columns(project, identifier):
    value = get_tblproperty(project, identifier, "clusteringColumns")
    return LiquidClusteringProcessor.extract_cluster_by(value)


def get_history_operations(project, identifier):
    rows = project.run_sql(
        f"select operation from (describe history {{database}}.{{schema}}.{identifier})",
        fetch="all",
    )
    return [row[0] for row in rows]


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestTableLiquidClusteringEffect:
    """V1 table CTAS with liquid_clustered_by must produce a clustered table and OPTIMIZE it."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_liquid_cluster.sql": fixtures.table_liquid_cluster_sql,
        }

    def test_clustering_columns_set(self, project):
        util.run_dbt(["run"])
        assert get_clustering_columns(project, "table_liquid_cluster") == ["id"]
        assert "OPTIMIZE" in get_history_operations(project, "table_liquid_cluster")


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestTableV2LiquidClusteringEffect(MaterializationV2Mixin):
    """V2 table create must carry liquid_clustered_by into the created table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_liquid_cluster.sql": fixtures.table_liquid_cluster_sql,
        }

    def test_clustering_columns_set(self, project):
        util.run_dbt(["run"])
        assert get_clustering_columns(project, "table_liquid_cluster") == ["id"]


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestAutoLiquidClusteringTableEffect:
    """auto_liquid_cluster=true must create the table with CLUSTER BY AUTO."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "auto_liquid_cluster_table.sql": fixtures.auto_liquid_cluster_table_sql,
        }

    def test_cluster_by_auto_set(self, project):
        util.run_dbt(["run"])
        assert get_tblproperty(project, "auto_liquid_cluster_table", "clusterByAuto") == "true"


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalSwitchToAutoCluster(RerunSafeMixin):
    """Changing clustering config on an existing incremental model must be applied via ALTER:
    explicit columns -> auto (CLUSTER BY AUTO) -> removed (CLUSTER BY NONE)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_switch.sql": fixtures.incremental_three_cols_sql,
            "schema.yml": fixtures.cluster_switch_schema_cols,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("cluster_switch",)

    def test_switch_to_auto_then_none(self, project):
        util.run_dbt(["run"])
        assert get_clustering_columns(project, "cluster_switch") == ["id"]

        util.write_file(fixtures.cluster_switch_schema_auto, "models", "schema.yml")
        util.run_dbt(["run"])
        assert get_tblproperty(project, "cluster_switch", "clusterByAuto") == "true"

        util.write_file(fixtures.cluster_switch_schema_plain, "models", "schema.yml")
        util.run_dbt(["run"])
        assert get_tblproperty(project, "cluster_switch", "clusterByAuto") != "true"
        assert not get_clustering_columns(project, "cluster_switch")


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestIncrementalV2LiquidClusteringChange(RerunSafeMixin, MaterializationV2Mixin):
    """With use_materialization_v2, a liquid_clustered_by change on an incremental model
    must be applied via the V2 config-changeset ALTER path."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cluster_change.sql": fixtures.incremental_three_cols_sql,
            "schema.yml": fixtures.cluster_change_schema_initial,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("cluster_change",)

    def test_changing_cluster_by(self, project):
        util.run_dbt(["run"])
        assert get_clustering_columns(project, "cluster_change") == ["id"]

        util.write_file(fixtures.cluster_change_schema_updated, "models", "schema.yml")
        util.run_dbt(["run"])
        assert get_clustering_columns(project, "cluster_change") == ["msg", "color"]
