import pytest
from dbt.tests import util
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization

from tests.functional.adapter.basic import fixtures
from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    MaterializationV2Mixin,
    RerunSafeMixin,
)


class TestTableMat(BaseTableMaterialization):
    pass


class BaseTableRerunReplacesInPlace(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"rerun_target.sql": fixtures.rerun_target_initial_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("rerun_target",)

    def test_rerun_replaces_in_place(self, project):
        util.run_dbt(["run"])
        history_before = project.run_sql(
            "describe history {database}.{schema}.rerun_target", fetch="all"
        )

        util.write_file(fixtures.rerun_target_updated_sql, "models", "rerun_target.sql")
        util.run_dbt(["run"])
        history_after = project.run_sql(
            "describe history {database}.{schema}.rerun_target", fetch="all"
        )

        # Replacing in place appends to the existing table history; dropping and
        # recreating would reset it, so the history must have grown across the rerun.
        assert len(history_after) > len(history_before)

        rows = project.run_sql("select id, msg from {database}.{schema}.rerun_target", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert rows[0][1] == "second"


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestTableRerunReplacesInPlace(BaseTableRerunReplacesInPlace, MaterializationV1Mixin):
    pass


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestTableRerunReplacesInPlaceV2(BaseTableRerunReplacesInPlace, MaterializationV2Mixin):
    pass


class TestTableReplacesExistingView(RerunSafeMixin, MaterializationV1Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"convertible.sql": fixtures.convertible_view_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("convertible",)

    def test_view_is_replaced_by_table(self, project):
        util.run_dbt(["run"])
        util.check_relation_types(project.adapter, {"convertible": "view"})

        util.write_file(fixtures.convertible_table_sql, "models", "convertible.sql")
        util.run_dbt(["run"])
        util.check_relation_types(project.adapter, {"convertible": "table"})

        rows = project.run_sql("select count(*) from {database}.{schema}.convertible", fetch="one")
        assert rows[0] == 2


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

    @pytest.mark.skip_profile("databricks_cluster")
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
