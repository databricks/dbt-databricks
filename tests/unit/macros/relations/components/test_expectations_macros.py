from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.expectations import ExpectationsConfig
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from tests.unit.macros.base import MacroTestBase

NOT_TWO = ExpectationsConfig(
    expectations=[{"name": "not_two", "expression": "id <> 2", "on_violation": "drop"}],
    fingerprint="abc123",
)


def column(rendered):
    return Mock(**{"render_for_create.return_value": rendered})


class TestExpectationsMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "expectations.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]

    def test_get_create_sql_expectations__empty(self, template_bundle):
        sql = self.run_macro_raw(
            template_bundle.template, "get_create_sql_expectations", ExpectationsConfig()
        )
        assert sql == ""

    def test_get_create_sql_expectations__actions(self, template_bundle):
        expectations = ExpectationsConfig(
            expectations=[
                {"name": "Known_Id", "expression": "id IN (1, 2)", "on_violation": "warn"},
                {"name": "positive_amount", "expression": "amount > 0", "on_violation": "drop"},
                {
                    "name": "known_currency",
                    "expression": "currency = 'USD'",
                    "on_violation": "fail",
                },
            ]
        )
        sql = self.run_macro_raw(
            template_bundle.template, "get_create_sql_expectations", expectations
        )
        assert sql == (
            "CONSTRAINT Known_Id EXPECT (id IN (1, 2)), "
            "CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW, "
            "CONSTRAINT known_currency EXPECT (currency = 'USD') ON VIOLATION FAIL UPDATE"
        )


class TestGetColumnAndConstraintsSql(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "relation.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/adapters"]

    @pytest.mark.parametrize(
        "expectations, create_constraints, expected",
        [
            ("", [], "(id int, v int)"),
            ("", ["pk"], "(id int, v int, primary key (id))"),
            ("expect (v > 0)", [], "(id int, v int, expect (v > 0))"),
            ("expect (v > 0)", ["pk"], "(id int, v int, expect (v > 0), primary key (id))"),
        ],
    )
    def test_get_column_and_constraints_sql__separators(
        self, template_bundle, expectations, create_constraints, expected
    ):
        relation = template_bundle.relation
        relation.create_constraints = create_constraints
        relation.render_constraints_for_create.return_value = "primary key (id)"

        sql = self.run_macro(
            template_bundle.template,
            "get_column_and_constraints_sql",
            relation,
            [column("id int"), column("v int")],
            expectations,
        )

        self.assert_sql_equal(sql, expected)


class _CreateMacrosBase(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["relation.sql", "expectations.sql"]

    @pytest.fixture(autouse=True)
    def create_context(self, context):
        self.relation_config = {
            "partition_by": PartitionedByConfig(partition_by=[]),
            "tblproperties": TblPropertiesConfig(tblproperties={"key": "value"}),
            "comment": CommentConfig(),
            "refresh": RefreshConfig(),
            "expectations": ExpectationsConfig(),
        }
        context["adapter"].get_config_from_model.return_value.config = self.relation_config
        context["adapter"].parse_columns_and_constraints.return_value = ([column("id bigint")], [])
        context["model"].get = lambda key, default=None: default
        context["make_temp_relation"] = Mock()
        context["statement"] = lambda name, caller=None, **kwargs: ""
        context["column_mask_exists"] = Mock(return_value=False)
        for macro in (
            "create_temporary_view",
            "get_create_row_filter_clause",
            "get_create_sql_partition_by",
            "liquid_clustered_cols",
            "get_create_sql_comment",
            "get_create_sql_tblproperties",
            "get_create_sql_refresh_schedule",
        ):
            context[macro] = Mock(return_value="")

    EXPECTED = [
        (ExpectationsConfig(), "(id bigint)", {"key": "value"}),
        (
            NOT_TWO,
            "(id bigint, constraint not_two expect (id <> 2) on violation drop row)",
            {"key": "value", "dbt.expectations_hash": "abc123"},
        ),
    ]


class TestCreateStreamingTableExpectations(_CreateMacrosBase):
    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return [
            "macros/relations/streaming_table",
            "macros/relations/components",
            "macros/relations",
            "macros/adapters",
        ]

    @pytest.mark.parametrize("expectations, column_list, tblproperties", _CreateMacrosBase.EXPECTED)
    def test_create_streaming_table__expectations(
        self, template_bundle, context, expectations, column_list, tblproperties
    ):
        self.relation_config["expectations"] = expectations
        template_bundle.relation.create_constraints = []

        sql = self.render_bundle(
            template_bundle, "databricks__get_create_streaming_table_as_sql", "select 1"
        )

        self.assert_sql_equal(
            sql,
            f"create streaming table `some_database`.`some_schema`.`some_table` {column_list} "
            "as select 1",
        )
        context["get_create_sql_tblproperties"].assert_called_once_with(tblproperties)


class TestCreateMaterializedViewExpectations(_CreateMacrosBase):
    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return [
            "macros/relations/materialized_view",
            "macros/relations/components",
            "macros/relations",
            "macros/adapters",
        ]

    @pytest.mark.parametrize("expectations, column_list, tblproperties", _CreateMacrosBase.EXPECTED)
    def test_create_materialized_view__expectations(
        self, template_bundle, context, expectations, column_list, tblproperties
    ):
        self.relation_config["expectations"] = expectations
        target_relation = template_bundle.relation.enrich.return_value
        target_relation.render.return_value = template_bundle.relation.render()
        target_relation.create_constraints = []

        sql = self.render_bundle(
            template_bundle, "databricks__get_create_materialized_view_as_sql", "select 1"
        )

        self.assert_sql_equal(
            sql,
            "create or replace materialized view `some_database`.`some_schema`.`some_table` "
            f"{column_list} as select 1",
        )
        context["get_create_sql_tblproperties"].assert_called_once_with(tblproperties)
