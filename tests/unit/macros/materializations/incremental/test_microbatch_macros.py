from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestMicrobatchMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "materializations/incremental/strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/materializations/incremental"]

    @pytest.fixture
    def mock_temp(self):
        relation = Mock()
        relation.identifier = "temp_table"
        relation.render = Mock(return_value="`temp_table`")
        relation.type = "table"
        return relation

    @pytest.fixture
    def mock_arg_dict(self, relation, mock_temp):
        return {
            "temp_relation": mock_temp,
            "target_relation": relation,
            "unique_key": "id",
            "incremental_predicates": None,
        }

    def test_databricks__get_incremental_microbatch_sql_with_start_and_end(
        self, template_bundle, context, config, mock_arg_dict
    ):
        context["model"] = {"config": {"event_time": "event_timestamp"}}
        config["__dbt_internal_microbatch_event_time_start"] = "2023-01-01 00:00:00"
        config["__dbt_internal_microbatch_event_time_end"] = "2023-01-02 00:00:00"

        context["return"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_incremental_microbatch_sql",
            mock_arg_dict,
        )

        result = context["return"].call_args[0][0]

        expected = """
        insert into `some_database`.`some_schema`.`some_table`
        replace where cast(event_timestamp as TIMESTAMP) >= '2023-01-01 00:00:00'
            and cast(event_timestamp as TIMESTAMP) < '2023-01-02 00:00:00' table `temp_table`
"""

        self.assert_sql_equal(result, expected)

    def test_databricks__get_incremental_microbatch_sql_with_start_only(
        self, template_bundle, context, config, mock_arg_dict
    ):
        context["model"] = {"config": {"event_time": "event_timestamp"}}
        config["__dbt_internal_microbatch_event_time_start"] = "2023-01-01 00:00:00"
        config["__dbt_internal_microbatch_event_time_end"] = None

        context["return"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_incremental_microbatch_sql",
            mock_arg_dict,
        )

        result = context["return"].call_args[0][0]

        expected = """
        insert into `some_database`.`some_schema`.`some_table`
        replace where cast(event_timestamp as TIMESTAMP) >= '2023-01-01 00:00:00' table `temp_table`
"""

        self.assert_sql_equal(result, expected)

    def test_databricks__get_incremental_microbatch_sql_with_end_only(
        self, template_bundle, context, config, mock_arg_dict
    ):
        context["model"] = {"config": {"event_time": "event_timestamp"}}
        config["__dbt_internal_microbatch_event_time_start"] = None
        config["__dbt_internal_microbatch_event_time_end"] = "2023-01-02 00:00:00"

        context["return"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_incremental_microbatch_sql",
            mock_arg_dict,
        )

        result = context["return"].call_args[0][0]

        expected = """
        insert into `some_database`.`some_schema`.`some_table`
        replace where cast(event_timestamp as TIMESTAMP) < '2023-01-02 00:00:00' table `temp_table`
"""

        self.assert_sql_equal(result, expected)

    def test_databricks__get_incremental_microbatch_sql_with_existing_predicates(
        self, template_bundle, context, config, mock_arg_dict
    ):
        mock_arg_dict["incremental_predicates"] = ["col1 = 'value'", "col2 > 100"]

        context["model"] = {"config": {"event_time": "event_timestamp"}}
        config["__dbt_internal_microbatch_event_time_start"] = "2023-01-01 00:00:00"
        config["__dbt_internal_microbatch_event_time_end"] = "2023-01-02 00:00:00"

        context["return"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_incremental_microbatch_sql",
            mock_arg_dict,
        )

        result = context["return"].call_args[0][0]

        expected = """
        insert into `some_database`.`some_schema`.`some_table`
        replace where col1 = 'value'
            and col2 > 100 and cast(event_timestamp as TIMESTAMP) >= '2023-01-01 00:00:00'
            and cast(event_timestamp as TIMESTAMP) < '2023-01-02 00:00:00' table `temp_table`
"""

        self.assert_sql_equal(result, expected)

    def test_databricks__get_incremental_microbatch_sql_without_start_or_end(
        self, template_bundle, context, config, mock_arg_dict
    ):
        context["model"] = {"config": {"event_time": "event_timestamp"}}
        config["__dbt_internal_microbatch_event_time_start"] = None
        config["__dbt_internal_microbatch_event_time_end"] = None

        context["return"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_incremental_microbatch_sql",
            mock_arg_dict,
        )

        result = context["return"].call_args[0][0]

        expected = """
        insert into `some_database`.`some_schema`.`some_table`
        table `temp_table`
"""

        self.assert_sql_equal(result, expected)
