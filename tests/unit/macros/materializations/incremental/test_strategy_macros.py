import re

import pytest
from agate import Date, Number, Table, Text
from mock.mock import Mock

from tests.unit.macros.base import MacroTestBase


class TestStrategytMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    def run_add_predicates_macro(
        self, template, context, predicates, partition_columns
    ) -> str:
        context["adapter"].quote.side_effect = lambda x: x
        context["convert_to_list"] = lambda x, default_value=None: x if x else []
        return self.run_macro(
            template,
            "add_dest_table_partition_predicates",
            predicates,
            partition_columns,
            "test_relation",
        )

    @pytest.mark.parametrize("partition_columns", [["id"], [], None, "id"])
    def test_add_dest_table_partition_predicates_returns_input(
        self, template, context, partition_columns
    ):
        context["execute"] = False
        context["run_query"] = Mock(return_value=[])
        predicates = ["test", "123", "456"]
        expected = f"{predicates} {predicates}"

        actual = self.run_add_predicates_macro(
            template, context, predicates, partition_columns
        )

        assert actual == expected

    @pytest.mark.parametrize("partition_columns", [[], None])
    def test_add_dest_table_partition_predicates_no_query_with_no_partition_columns(
        self, template, context, partition_columns
    ):
        context["execute"] = True
        context["run_query"] = Mock(return_value=[])
        predicates = ["test", "123", "456"]
        expected = f"{predicates} {predicates}"

        actual = self.run_add_predicates_macro(
            template, context, predicates, partition_columns
        )

        assert actual == expected

    def test_add_dest_table_partition_predicates_correct_query(self, template, context):
        partition_columns = ["date", "code", "id"]
        soure_name = "test_relation"
        partition_query_statements = ",\n\t    ".join(
            [f"MIN({c})," + "\n\t    " + f"MAX({c})" for c in partition_columns]
        )
        expected_query = f"""
        select
            {partition_query_statements}
        from {soure_name}"""
        partition_column_return = ["2024-01-01", "2024-01-10", "a", "z", 0, 9832]
        partition_column_output_formatted = [
            "'2024-01-01'",
            "'2024-01-10'",
            "'a'",
            "'z'",
            0,
            9832,
        ]
        run_query_return = Table(
            [partition_column_return],
            column_names=[f"c{n}" for n in range(len(partition_column_return))],
            column_types=[Date(), Date(), Text(), Text(), Number(), Number()],
        )

        def side_effect(arg):
            if (
                re.sub(r"\s\s+", " ", arg).strip()
                == re.sub(r"\s\s+", " ", expected_query).strip()
            ):
                return run_query_return

            raise RuntimeError(f"Unexpected query: {arg}")

        context["execute"] = True
        context["run_query"] = Mock(side_effect=side_effect)
        predicates = None
        expected_predicates = []

        for i, col_name in enumerate(partition_columns):
            min_val = partition_column_output_formatted[i * 2]
            max_val = partition_column_output_formatted[i * 2 + 1]
            expected_predicates += [
                f" DBT_INTERNAL_DEST.{col_name} >= {min_val} "
                f"and DBT_INTERNAL_DEST.{col_name} <= {max_val}"
            ]

        expected = f"{expected_predicates}"

        actual = self.run_add_predicates_macro(
            template, context, predicates, partition_columns
        ).replace(r"\n", "")

        assert actual == expected

    @pytest.mark.parametrize(
        "func_input, expected", [(1, "1"), (None, "NULL"), ("1", "'1'"), (True, "True")]
    )
    def test_to_literal(self, template, func_input, expected):
        actual = self.run_macro(template, "to_literal", func_input)

        assert actual == expected
