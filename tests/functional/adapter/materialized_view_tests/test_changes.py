from typing import Optional

import pytest

from dbt.adapters.base import BaseRelation
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.tests import util
from dbt.tests.adapter.materialized_view.changes import (
    MaterializedViewChanges,
    MaterializedViewChangesApplyMixin,
    MaterializedViewChangesContinueMixin,
    MaterializedViewChangesFailMixin,
)
from tests.functional.adapter.materialized_view_tests import fixtures


def _check_tblproperties(tblproperties: TblPropertiesConfig, expected: dict):
    final_tblproperties = {
        k: v for k, v in tblproperties.tblproperties.items() if k not in tblproperties.ignore_list
    }
    assert final_tblproperties == expected


class MaterializedViewChangesMixin(MaterializedViewChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {"my_materialized_view.sql": fixtures.materialized_view}

    @staticmethod
    def check_start_state(project, materialized_view):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(materialized_view)
        assert isinstance(results, MaterializedViewConfig)
        assert results.config["partition_by"].partition_by == ["id"]
        assert results.config["query"].query.startswith("select * from")
        _check_tblproperties(results.config["tblproperties"], {"key": "value"})
        assert results.config["refresh"].cron == "0 0 * * * ? *"
        assert results.config["refresh"].time_zone_value == "Etc/UTC"

    @staticmethod
    def change_config_via_alter(project, materialized_view):
        initial_model = util.get_model_file(project, materialized_view)
        new_model = initial_model.replace("'cron': '0 0 * * * ? *'", "'cron': '0 5 * * * ? *'")
        util.set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, materialized_view):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(materialized_view)
        assert isinstance(results, MaterializedViewConfig)
        assert results.config["refresh"].cron == "0 5 * * * ? *"
        assert results.config["refresh"].time_zone_value == "Etc/UTC"

    @staticmethod
    def change_config_via_replace(project, materialized_view):
        initial_model = util.get_model_file(project, materialized_view)
        new_model = (
            initial_model.replace("partition_by='id',", "")
            .replace("select *", "select id, value")
            .replace("'key': 'value'", "'other': 'other'")
        )
        util.set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_replace_change_is_applied(project, materialized_view):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(materialized_view)
        assert isinstance(results, MaterializedViewConfig)
        assert results.config["partition_by"].partition_by == []
        assert results.config["query"].query.startswith("select id, value")
        _check_tblproperties(results.config["tblproperties"], {"other": "other"})

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewApplyChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesApplyMixin
):
    pass


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewContinueOnChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesContinueMixin
):
    pass


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewFailOnChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesFailMixin
):
    pass
