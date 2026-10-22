import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import MaterializationV2Mixin, RerunSafeMixin
from tests.functional.adapter.relations import fixtures


def _describe_detail(project, identifier):
    rows = project.run_sql(f"describe detail {{database}}.{{schema}}.{identifier}", fetch="all")
    return rows[0]


@pytest.mark.skip_profile("databricks_cluster")
class TestSaferOpsMintsNewObject(RerunSafeMixin, MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"safer_ops_model.sql": fixtures.safer_ops_table_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("safer_ops_model",)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True},
        }

    def test_replace_mints_new_object(self, project):
        util.run_dbt(["run"])
        first_id = _describe_detail(project, "safer_ops_model")["id"]
        util.run_dbt(["run"])
        second_id = _describe_detail(project, "safer_ops_model")["id"]
        assert first_id != second_id, (
            "use_safer_relation_operations should change the Delta id on replacement; "
            f"id stayed {first_id}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestSaferOpsAsBoolIncremental(RerunSafeMixin, MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"asbool_incremental.sql": fixtures.safer_ops_incremental_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("asbool_incremental",)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": "false"},
        }

    def test_string_false_does_not_swap_on_incremental(self, project):
        util.run_dbt(["run"])
        first_id = _describe_detail(project, "asbool_incremental")["id"]
        util.run_dbt(["run"])
        second_id = _describe_detail(project, "asbool_incremental")["id"]
        assert first_id == second_id, (
            "incremental reads use_safer_relation_operations | as_bool; string 'false' should "
            f"be False and not swap; Delta id changed {first_id} -> {second_id}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestSaferOpsReplacesViewAfterUpstreamColumnRename(RerunSafeMixin, MaterializationV2Mixin):
    file_format = "delta"
    upstream_type = "table"

    @pytest.fixture(scope="class")
    def models(self):
        models = {"safer_ops_view.sql": fixtures.safer_ops_view_sql}
        if self.upstream_type == "table":
            models["safer_ops_upstream.sql"] = fixtures.safer_ops_upstream_sql
        return models

    @pytest.fixture(scope="class")
    def seeds(self):
        if self.upstream_type == "seed":
            return {"safer_ops_upstream.csv": fixtures.safer_ops_upstream_csv}
        return {}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return (
            "safer_ops_view",
            "safer_ops_view__dbt_stg",
            "safer_ops_view__dbt_backup",
            "safer_ops_upstream",
        )

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True, "+file_format": self.file_format},
        }

    def test_view_is_replaced(self, project):
        if self.upstream_type == "seed":
            util.write_file(fixtures.safer_ops_upstream_csv, "seeds", "safer_ops_upstream.csv")
        util.run_dbt(["build"])
        if self.upstream_type == "seed":
            util.write_file(
                fixtures.safer_ops_upstream_renamed_csv, "seeds", "safer_ops_upstream.csv"
            )
        else:
            util.write_file(
                fixtures.safer_ops_upstream_renamed_sql, "models", "safer_ops_upstream.sql"
            )
        util.write_file(fixtures.safer_ops_view_renamed_sql, "models", "safer_ops_view.sql")
        util.run_dbt(["build", "--full-refresh"] if self.upstream_type == "seed" else ["run"])
        rows = project.run_sql(
            "select id, new_col from {database}.{schema}.safer_ops_view", fetch="all"
        )
        assert [tuple(row) for row in rows] == [(1, "a")]

        util.write_file(fixtures.flip_relation_as_invalid_view_sql, "models", "safer_ops_view.sql")
        util.run_dbt(["run", "-s", "safer_ops_view"], expect_pass=False)
        rows = project.run_sql(
            "select id, new_col from {database}.{schema}.safer_ops_view", fetch="all"
        )
        assert [tuple(row) for row in rows] == [(1, "a")]
        artifacts = project.run_sql(
            "select table_name from {database}.information_schema.tables "
            "where table_schema = '{schema}' and table_name in "
            "('safer_ops_view__dbt_stg', 'safer_ops_view__dbt_backup')",
            fetch="all",
        )
        assert not artifacts


class TestSaferOpsReplacesParquetViewAfterUpstreamColumnRename(
    TestSaferOpsReplacesViewAfterUpstreamColumnRename
):
    file_format = "parquet"


class TestSaferOpsReplacesViewAfterSeedColumnRename(
    TestSaferOpsReplacesViewAfterUpstreamColumnRename
):
    upstream_type = "seed"


class TestSaferOpsReplacesParquetViewAfterSeedColumnRename(
    TestSaferOpsReplacesViewAfterSeedColumnRename
):
    file_format = "parquet"
