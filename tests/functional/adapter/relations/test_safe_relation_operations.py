"""Server-observable behavior of use_safer_relation_operations.

* The staging-swap mints a new UC object on each replace (the Delta id changes), which
  breaks history/lineage.
* incremental.sql reads the config with `| as_bool`, so a YAML string "false" is correctly
  treated as False (in-place, stable Delta id).
* A view is replaced even after its upstream dropped a column the old view selects, because
  view-to-view replaces use CREATE OR REPLACE VIEW instead of renaming the old view.
"""

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
        util.run_dbt(["run"])  # second run replaces via staging-swap
        second_id = _describe_detail(project, "safer_ops_model")["id"]
        assert first_id != second_id, (
            "use_safer_relation_operations should mint a new UC object on replace "
            f"(history broken); Delta id stayed {first_id}"
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
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "safer_ops_upstream.sql": fixtures.safer_ops_upstream_sql,
            "safer_ops_view.sql": fixtures.safer_ops_view_sql,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("safer_ops_view", "safer_ops_upstream")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True},
        }

    def test_view_is_replaced(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.safer_ops_upstream_renamed_sql, "models", "safer_ops_upstream.sql")
        util.write_file(fixtures.safer_ops_view_renamed_sql, "models", "safer_ops_view.sql")
        util.run_dbt(["run"])
        rows = project.run_sql(
            "select id, new_col from {database}.{schema}.safer_ops_view", fetch="all"
        )
        assert [tuple(row) for row in rows] == [(1, "a")]
