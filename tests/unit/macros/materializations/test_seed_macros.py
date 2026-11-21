from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestLoadCsvRows(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/seeds", "macros"]

    @pytest.fixture
    def agate_table(self):
        table = Mock()
        table.column_names = ["id", "name", "amount"]
        table.rows = [(1, "a", 1.5), (2, "b", 2.5), (3, "c", 3.5)]
        return table

    @pytest.fixture
    def seed_context(self, context, relation):
        context["get_batch_size"] = Mock(return_value=2)
        context["get_binding_char"] = Mock(return_value="%s")
        context["this"] = relation
        inferred = {0: "bigint", 1: "string", 2: "double"}
        context["adapter"].convert_type = Mock(side_effect=lambda _table, index: inferred[index])
        return context

    def render(self, template, file_format, agate_table):
        model = {
            "config": {"file_format": file_format, "column_types": {"amount": "decimal(10,2)"}}
        }
        return self.run_macro(template, "databricks__load_csv_rows", model, agate_table)

    def test_parquet_casts_each_column_to_its_resolved_type(
        self, template, seed_context, agate_table
    ):
        sql = self.render(template, "parquet", agate_table)

        row = "(cast(%s as bigint),cast(%s as string),cast(%s as decimal(10,2)))"
        self.assert_sql_equal(
            sql, f"insert overwrite `some_database`.`some_schema`.`some_table` values {row},{row}"
        )
        batches = seed_context["adapter"].add_query.call_args_list
        assert [call.kwargs["bindings"] for call in batches] == [
            [1, "a", 1.5, 2, "b", 2.5],
            [3, "c", 3.5],
        ]
        self.assert_sql_equal(
            batches[1].args[0],
            f"insert into `some_database`.`some_schema`.`some_table` values {row}",
        )

    def test_types_and_binding_char_resolve_once_per_load(
        self, template, seed_context, agate_table
    ):
        self.render(template, "parquet", agate_table)

        assert seed_context["adapter"].convert_type.call_count == len(agate_table.column_names)
        assert seed_context["get_binding_char"].call_count == 1

    def test_delta_uses_plain_placeholders(self, template, seed_context, agate_table):
        sql = self.render(template, "delta", agate_table)

        self.assert_sql_equal(
            sql,
            "insert overwrite `some_database`.`some_schema`.`some_table` values "
            "(%s,%s,%s),(%s,%s,%s)",
        )
        seed_context["adapter"].convert_type.assert_not_called()


class TestCreateCsvTable(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/seeds", "macros/relations", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return [
            "file_format.sql",
            "tblproperties.sql",
            "location.sql",
            "liquid_clustering.sql",
        ]

    @pytest.fixture
    def agate_table(self):
        table = Mock()
        table.column_names = ["id", "name"]
        return table

    @pytest.fixture
    def seed_context(self, context, relation):
        context["this"] = relation
        inferred = {0: "bigint", 1: "string"}
        context["adapter"].convert_type = Mock(side_effect=lambda _table, index: inferred[index])
        context["adapter"].quote_seed_column = Mock(side_effect=lambda col, quote: f"`{col}`")
        context["adapter"].is_uniform = Mock(return_value=False)
        context["adapter"].update_tblproperties_for_uniform_iceberg = Mock(return_value={})
        catalog_relation = Mock()
        catalog_relation.file_format = "delta"
        context["adapter"].build_catalog_relation = Mock(return_value=catalog_relation)
        return context

    def render(self, template, agate_table, model=None):
        if model is None:
            model = {"alias": "some_table", "config": {}}
        return self.run_macro(template, "create_or_replace_csv_table", model, agate_table)

    def test_create_table_with_liquid_clustered_by(
        self, template, seed_context, agate_table, config
    ):
        config["liquid_clustered_by"] = ["id", "name"]
        sql = self.render(template, agate_table)
        assert "cluster by (id, name)" in sql

    def test_create_table_with_auto_liquid_cluster(
        self, template, seed_context, agate_table, config
    ):
        config["auto_liquid_cluster"] = True
        sql = self.render(template, agate_table)
        assert "cluster by auto" in sql

    def test_create_table_without_liquid_clustering(
        self, template, seed_context, agate_table, config
    ):
        sql = self.render(template, agate_table)
        assert "cluster by" not in sql


class TestSeedMaterialization(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "seeds.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/seeds", "macros/relations", "macros"]

    @pytest.fixture
    def databricks_env(self, macro_folders_to_load):
        from dbt_common.clients.jinja import MaterializationExtension

        databricks_env = MacroTestBase.databricks_env.__wrapped__(self, macro_folders_to_load)
        databricks_env.add_extension(MaterializationExtension)
        return databricks_env

    @pytest.fixture
    def materialization_context(self, context, relation):
        context["this"] = relation
        context["model"] = {"alias": relation.identifier}
        context["should_full_refresh"] = Mock(return_value=False)
        context["database"] = relation.database
        context["schema"] = relation.schema
        context["load_agate_table"] = Mock()
        context["store_result"] = Mock()
        context["run_pre_hooks"] = Mock()
        context["create_csv_table"] = Mock(return_value="")
        context["reset_csv_table"] = Mock(return_value="")
        context["load_csv_rows"] = Mock(return_value="")
        context["log_seed_operation"] = Mock()
        context["should_revoke"] = Mock(return_value=False)
        context["apply_grants"] = Mock()
        context["apply_tags"] = Mock()
        context["apply_column_tags"] = Mock()
        context["optimize"] = Mock()
        context["run_post_hooks"] = Mock()
        context["adapter"].get_relation = Mock(return_value=None)
        context["adapter"].get_column_tags_from_model = Mock(return_value=None)
        return context

    def test_seed_materialization_with_tags_and_optimize(
        self, template, materialization_context, config
    ):
        config["databricks_tags"] = {"tier": "gold"}
        col_tags = Mock(set_column_tags={"id": {"pii": "false"}})
        materialization_context["adapter"].get_column_tags_from_model.return_value = col_tags

        macro = getattr(template.module, "dbt_macro__materialization_seed_databricks")
        macro()

        materialization_context["apply_tags"].assert_called_once_with(
            materialization_context["this"].incorporate.return_value, {"tier": "gold"}
        )
        materialization_context["apply_column_tags"].assert_called_once_with(
            materialization_context["this"].incorporate.return_value, col_tags
        )
        materialization_context["optimize"].assert_called_once_with(
            materialization_context["this"].incorporate.return_value
        )

    def test_seed_materialization_without_tags(self, template, materialization_context, config):
        macro = getattr(template.module, "dbt_macro__materialization_seed_databricks")
        macro()

        materialization_context["apply_tags"].assert_not_called()
        materialization_context["apply_column_tags"].assert_not_called()
        materialization_context["optimize"].assert_called_once_with(
            materialization_context["this"].incorporate.return_value
        )
