from typing import Optional

import pytest
from dbt_common.contracts.config.materialization import OnConfigurationChangeOption

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import (
    MY_SEED,
)
from tests.functional.adapter.streaming_tables import fixtures


def _check_tblproperties(tblproperties: TblPropertiesConfig, expected: dict):
    final_tblproperties = {
        k: v for k, v in tblproperties.tblproperties.items() if k not in tblproperties.ignore_list
    }
    assert final_tblproperties == expected


class StreamingTableChanges:
    @staticmethod
    def check_start_state(project, streaming_table):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(streaming_table)
        assert isinstance(results, StreamingTableConfig)
        assert results.config["partition_by"].partition_by == ["id"]
        _check_tblproperties(results.config["tblproperties"], {"key": "value"})
        assert results.config["refresh"].cron == "0 0 * * * ? *"
        assert results.config["refresh"].time_zone_value == "Etc/UTC"

    @staticmethod
    def change_config_via_alter(project, streaming_table):
        initial_model = util.get_model_file(project, streaming_table)
        new_model = initial_model.replace(
            "'cron': '0 0 * * * ? *'", "'cron': '0 5 * * * ? *'"
        ).replace("'key': 'value'", "'pipeline': 'altered'")
        util.set_model_file(project, streaming_table, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, streaming_table):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(streaming_table)
        assert isinstance(results, StreamingTableConfig)
        assert results.config["refresh"].cron == "0 5 * * * ? *"
        assert results.config["refresh"].time_zone_value == "Etc/UTC"
        _check_tblproperties(results.config["tblproperties"], {"pipeline": "altered"})

    @staticmethod
    def change_config_via_replace(project, streaming_table):
        initial_model = util.get_model_file(project, streaming_table)
        new_model = initial_model.replace("partition_by='id'", "partition_by='value'")
        util.set_model_file(project, streaming_table, new_model)

    @staticmethod
    def check_state_replace_change_is_applied(project, streaming_table):
        with util.get_connection(project.adapter):
            results = project.adapter.get_relation_config(streaming_table)
        assert isinstance(results, StreamingTableConfig)
        assert results.config["partition_by"].partition_by == ["value"]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_streaming_table.sql": fixtures.complex_streaming_table}

    @pytest.fixture(scope="class")
    def my_streaming_table(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_streaming_table",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.StreamingTable,
        )

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_streaming_table):
        # make sure the model in the data reflects the files each time
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", my_streaming_table.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = util.get_model_file(project, my_streaming_table)

        yield

        # and then reset them after the test runs
        util.set_model_file(project, my_streaming_table, initial_model)

        # ensure clean slate each method
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_full_refresh_occurs_with_changes(self, project, my_streaming_table):
        StreamingTableChanges.change_config_via_alter(project, my_streaming_table)
        StreamingTableChanges.change_config_via_replace(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(
            [
                "--debug",
                "run",
                "--models",
                my_streaming_table.identifier,
                "--full-refresh",
            ]
        )
        assert (
            StreamingTableChanges.query_relation_type(project, my_streaming_table)
            == "streaming_table"
        )
        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs, False)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableChangesApply(StreamingTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Apply.value}}

    def test_change_is_applied_via_alter(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", my_streaming_table.name])

        self.check_state_alter_change_is_applied(project, my_streaming_table)

        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs, False)

    def test_change_is_applied_via_replace(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        self.change_config_via_replace(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", my_streaming_table.name])

        self.check_state_alter_change_is_applied(project, my_streaming_table)
        self.check_state_replace_change_is_applied(project, my_streaming_table)

        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableChangesContinue(StreamingTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Continue.value}}

    def test_change_is_not_applied_via_alter(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", my_streaming_table.name])

        self.check_start_state(project, my_streaming_table)

        util.assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `continue` for `{my_streaming_table}`",
            logs,
        )
        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs, False)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs, False)

    def test_change_is_not_applied_via_replace(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        self.change_config_via_replace(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(["--debug", "run", "--models", my_streaming_table.name])

        self.check_start_state(project, my_streaming_table)

        util.assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `continue` for `{my_streaming_table}`",
            logs,
        )
        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs, False)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs, False)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableChangesFail(StreamingTableChanges):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": OnConfigurationChangeOption.Fail.value}}

    def test_change_is_not_applied_via_alter(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(
            ["--debug", "run", "--models", my_streaming_table.name], expect_pass=False
        )

        self.check_start_state(project, my_streaming_table)

        util.assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `fail` for `{my_streaming_table}`",
            logs,
        )
        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs, False)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs, False)

    def test_change_is_not_applied_via_replace(self, project, my_streaming_table):
        self.check_start_state(project, my_streaming_table)

        self.change_config_via_alter(project, my_streaming_table)
        self.change_config_via_replace(project, my_streaming_table)
        _, logs = util.run_dbt_and_capture(
            ["--debug", "run", "--models", my_streaming_table.name], expect_pass=False
        )

        self.check_start_state(project, my_streaming_table)

        util.assert_message_in_logs(
            f"Configuration changes were identified and `on_configuration_change` was set"
            f" to `fail` for `{my_streaming_table}`",
            logs,
        )
        util.assert_message_in_logs(f"Applying ALTER to: {my_streaming_table}", logs, False)
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs, False)
