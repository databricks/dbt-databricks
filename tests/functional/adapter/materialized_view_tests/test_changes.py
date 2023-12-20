from typing import Optional
from dbt.tests.adapter.materialized_view.changes import (
    MaterializedViewChanges,
    MaterializedViewChangesApplyMixin,
    MaterializedViewChangesContinueMixin,
    MaterializedViewChangesFailMixin,
)
from dbt.adapters.base import BaseRelation
from dbt.tests import util
import pytest
from dbt.adapters.databricks.relation_configs.materialized_view import MaterializedViewConfig
from dbt.adapters.databricks.relation_configs.refresh import ScheduledRefreshConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig

from tests.functional.adapter.materialized_view_tests import fixtures


def _check_tblproperties(tblproperties: TblPropertiesConfig, expected: dict):
    assert tblproperties._without_ignore_list(tblproperties.tblproperties) == expected


class MaterializedViewChangesMixin(MaterializedViewChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {"my_materialized_view.sql": fixtures.materialized_view}

    @staticmethod
    def check_start_state(project, materialized_view):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(materialized_view)
        assert isinstance(results, MaterializedViewConfig)
        assert results.partition_by.partition_by == ["id"]
        assert results.query.query.startswith("select * from")
        _check_tblproperties(results.tblproperties, {"key": "value"})
        assert isinstance(results.refresh, ScheduledRefreshConfig)
        assert results.refresh.cron == "0 0 * * * ? *"
        assert results.refresh.time_zone_value == "Etc/UTC"

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
        assert isinstance(results.refresh, ScheduledRefreshConfig)
        assert results.refresh.cron == "0 5 * * * ? *"
        assert results.refresh.time_zone_value == "Etc/UTC"

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
        assert results.partition_by.partition_by == []
        assert results.query.query.startswith("select id, value")
        _check_tblproperties(results.tblproperties, {"other": "other"})

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)


class TestMaterializedViewApplyChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesApplyMixin
):
    pass


class TestMaterializedViewContinueOnChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesContinueMixin
):
    pass


class TestMaterializedViewFailOnChanges(
    MaterializedViewChangesMixin, MaterializedViewChangesFailMixin
):
    pass
