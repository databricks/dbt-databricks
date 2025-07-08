import pytest

from dbt.tests import util
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from tests.functional.adapter.basic import fixtures
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class TestTableMat(BaseTableMaterialization):
    pass


class TestAllColumnTypesV2(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"all_types_model.sql": fixtures.all_types_model_sql}

    def test_all_column_types_materialization(self, project):
        # Test table materialization with a large struct
        util.run_dbt(["run"])

        # Verify the table was created and contains data
        relation = util.relation_from_name(project.adapter, "all_types_model")
        results = project.run_sql(
            f"""
            DESCRIBE TABLE {relation};
            """,
            fetch="all",
        )
        # Check that the results match the expected columns and types
        expected = [
            ("tinyint_col", "tinyint"),
            ("smallint_col", "smallint"),
            ("int_col", "int"),
            ("bigint_col", "bigint"),
            ("float_col", "float"),
            ("double_col", "double"),
            ("decimal_col", "decimal(10,4)"),
            ("boolean_col", "boolean"),
            # It is expected that varchar and char end up being represented as strings in Databricks
            ("string_col", "string"),
            ("varchar_col", "string"),
            ("char_col", "string"),
            ("binary_col", "binary"),
            ("date_col", "date"),
            ("timestamp_col", "timestamp"),
            ("variant_col", "variant"),
            ("array_int_col", "array<int>"),
            ("map_col", "map<string,string>"),
            (
                "complex_struct",
                "struct<level1_field:int,level1_struct:struct<level2_field:string,level2_array:array<struct<level3_field:boolean>>>>",
            ),
        ]
        actual = [(row[0], row[1]) for row in results[: len(expected)]]
        assert actual == expected


class TestTruncatedStructV2(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"big_struct_model.sql": fixtures.big_struct_model_sql}

    def test_truncated_struct_materialization(self, project):
        util.run_dbt(["run"])
        results = project.run_sql(
            """
            SELECT COLUMN_NAME, FULL_DATA_TYPE FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = 'big_struct_model';
            """,
            fetch="all",
        )
        assert results[0][0] == "big_struct"
        expected_struct_type = "struct<" + ",".join([f"field{i}:int" for i in range(30)]) + ">"
        assert results[0][1] == expected_struct_type
