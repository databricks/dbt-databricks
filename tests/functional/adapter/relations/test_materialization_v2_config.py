"""Server-observable effects of choosing the materialization path per model.

* Table and incremental: the default path builds a new table with one CREATE ... AS SELECT; v2
  creates it empty and then inserts, so its history has a WRITE and no AS SELECT operation.
* View with view_update_via_alter: v2 leaves an unchanged view in place on rerun, so a property set
  outside dbt survives; the default path replaces the view and drops it.
"""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.relations import fixtures

OUT_OF_BAND_PROPERTY = "set_outside_dbt"


def _assert_built_by_v2(project, identifier, expected):
    rows = project.run_sql(f"describe history {{database}}.{{schema}}.{identifier}", fetch="all")
    operations = [row["operation"] for row in rows]
    built_by_v2 = "WRITE" in operations and not any(
        operation.endswith("AS SELECT") for operation in operations
    )
    assert built_by_v2 is expected, f"{identifier} history: {operations}"


def _rerun_keeps_out_of_band_property(project, identifier):
    project.run_sql(
        f"alter view {{database}}.{{schema}}.{identifier} "
        f"set tblproperties ('{OUT_OF_BAND_PROPERTY}' = 'present')"
    )
    util.run_dbt(["run", "--select", identifier])
    properties = dict(
        project.run_sql(f"show tblproperties {{database}}.{{schema}}.{identifier}", fetch="all")
    )
    return OUT_OF_BAND_PROPERTY in properties


class BaseMaterializationV2Config(RerunSafeMixin):
    model_setting: bool

    @pytest.fixture(scope="class")
    def relations_to_reset(self, models):
        return tuple(name.removesuffix(".sql") for name in models)

    def test_table_and_incremental_path(self, project):
        util.run_dbt(["run"])
        _assert_built_by_v2(project, "table_model", self.model_setting)
        _assert_built_by_v2(project, "incremental_model", self.model_setting)

    def test_view_path(self, project):
        util.run_dbt(["run"])
        assert _rerun_keeps_out_of_band_property(project, "view_model") is self.model_setting


@pytest.mark.skip_profile("databricks_cluster")
class TestProjectConfigOptsIntoV2(BaseMaterializationV2Config):
    model_setting = True

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {"+use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": fixtures.materialization_path_sql("table"),
            "incremental_model.sql": fixtures.materialization_path_sql("incremental"),
            "view_model.sql": fixtures.materialization_path_sql("view"),
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestModelConfigOptsOutOfV2(BaseMaterializationV2Config):
    model_setting = False

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": fixtures.materialization_path_sql("table", False),
            "incremental_model.sql": fixtures.materialization_path_sql("incremental", False),
            "view_model.sql": fixtures.materialization_path_sql("view", False),
            "follows_flag.sql": fixtures.materialization_path_sql("table"),
        }

    def test_unset_model_follows_flag(self, project):
        util.run_dbt(["run", "--select", "follows_flag"])
        _assert_built_by_v2(project, "follows_flag", True)
