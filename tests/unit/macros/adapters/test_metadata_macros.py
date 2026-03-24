from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestMetadataMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters/metadata.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/adapters"]

    @pytest.fixture
    def mock_schema_relation(self):
        relation = Mock()
        relation.database = "test_db"
        relation.schema = "test_schema"
        relation.render = Mock(return_value="`test_db`.`test_schema`")
        return relation

    @pytest.fixture
    def mock_information_schema(self):
        info_schema = Mock()
        info_schema.database = "test_db"
        info_schema.is_hive_metastore = Mock(return_value=False)
        return info_schema

    @pytest.fixture
    def mock_relations_list(self):
        relation1 = Mock()
        relation1.schema = "test_schema1"
        relation1.identifier = "test_table1"
        relation1.render = Mock(return_value="`test_schema1`.`test_table1`")

        relation2 = Mock()
        relation2.schema = "test_schema2"
        relation2.identifier = "test_table2"
        relation2.render = Mock(return_value="`test_schema2`.`test_table2`")

        return [relation1, relation2]

    def test_show_table_extended_sql(self, template_bundle, relation):
        result = self.run_macro(template_bundle.template, "show_table_extended_sql", relation)

        expected_sql = "SHOW TABLE EXTENDED IN `some_database`.`some_schema` LIKE 'some_table'"
        self.assert_sql_equal(result, expected_sql)

    def test_show_tables_sql(self, template_bundle, mock_schema_relation):
        result = self.run_macro(template_bundle.template, "show_tables_sql", mock_schema_relation)

        expected_sql = "SHOW TABLES IN `test_db`.`test_schema`"
        self.assert_sql_equal(result, expected_sql)

    def test_show_views_sql(self, template_bundle, mock_schema_relation):
        result = self.run_macro(template_bundle.template, "show_views_sql", mock_schema_relation)

        expected_sql = "SHOW VIEWS IN `test_db`.`test_schema`"
        self.assert_sql_equal(result, expected_sql)

    def test_get_relation_last_modified_sql_unity_catalog(
        self, context, template_bundle, mock_information_schema, mock_relations_list
    ):
        context["current_timestamp"] = Mock(return_value="current_timestamp()")
        mock_information_schema.is_hive_metastore.return_value = False

        result = self.run_macro(
            template_bundle.template,
            "get_relation_last_modified_sql",
            mock_information_schema,
            mock_relations_list,
        )

        expected_sql = """
            SELECT
              table_schema AS schema,
              table_name AS identifier,
              last_altered AS last_modified,
              current_timestamp() AS snapshotted_at
            FROM `system`.`information_schema`.`tables`
            WHERE table_catalog = 'test_db'
              AND (
                (table_schema = 'test_schema1' AND table_name = 'test_table1') OR
                (table_schema = 'test_schema2' AND table_name = 'test_table2')
              )
        """
        self.assert_sql_equal(result, expected_sql)

    def test_get_relation_last_modified_sql_hive_metastore(
        self, context, template_bundle, mock_information_schema, mock_relations_list
    ):
        context["current_timestamp"] = Mock(return_value="current_timestamp()")
        mock_information_schema.is_hive_metastore.return_value = True

        result = self.run_macro(
            template_bundle.template,
            "get_relation_last_modified_sql",
            mock_information_schema,
            mock_relations_list,
        )

        expected_sql = """
            SELECT
              'test_schema1' AS schema,
              'test_table1' AS identifier,
              max(timestamp) AS last_modified,
              current_timestamp() AS snapshotted_at
            FROM (DESCRIBE HISTORY `test_schema1`.`test_table1`)
            UNION ALL
            SELECT
              'test_schema2' AS schema,
              'test_table2' AS identifier,
              max(timestamp) AS last_modified,
              current_timestamp() AS snapshotted_at
            FROM (DESCRIBE HISTORY `test_schema2`.`test_table2`)
        """
        self.assert_sql_equal(result, expected_sql)

    def test_get_view_description_sql(self, template_bundle, relation):
        result = self.run_macro(template_bundle.template, "get_view_description_sql", relation)

        expected_sql = """
            SELECT *
            FROM `system`.`information_schema`.`views`
            WHERE table_catalog = 'some_database'
              AND table_schema = 'some_schema'
              AND table_name = 'some_table'
        """
        self.assert_sql_equal(result, expected_sql)

    def test_get_uc_tables_sql_with_identifier(self, template_bundle, relation):
        result = self.run_macro(template_bundle.template, "get_uc_tables_sql", relation)

        expected_sql = """
            SELECT
              table_name,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), 'table', lower(table_type)) AS table_type,
              lower(data_source_format) AS file_format,
              table_owner,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), lower(table_type), null) AS databricks_table_type
            FROM `system`.`information_schema`.`tables`
            WHERE table_catalog = 'some_database'
              AND table_schema = 'some_schema'
              AND table_name = 'some_table'
        """  # noqa
        self.assert_sql_equal(result, expected_sql)

    def test_get_uc_tables_sql_without_identifier(self, template_bundle, mock_schema_relation):
        mock_schema_relation.identifier = None

        result = self.run_macro(template_bundle.template, "get_uc_tables_sql", mock_schema_relation)

        expected_sql = """
            SELECT
              table_name,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), 'table', lower(table_type)) AS table_type,
              lower(data_source_format) AS file_format,
              table_owner,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), lower(table_type), null) AS databricks_table_type
            FROM `system`.`information_schema`.`tables`
            WHERE table_catalog = 'test_db'
              AND table_schema = 'test_schema'
        """  # noqa
        self.assert_sql_equal(result, expected_sql)

    def test_list_schemas_sql_with_database(self, template_bundle):
        result = self.run_macro(template_bundle.template, "list_schemas_sql", "my_catalog")

        expected_sql = "SHOW SCHEMAS IN `my_catalog`"
        self.assert_sql_equal(result, expected_sql)

    def test_list_schemas_sql_with_hyphenated_database(self, template_bundle):
        result = self.run_macro(
            template_bundle.template, "list_schemas_sql", "data_engineering-uc-dev"
        )

        expected_sql = "SHOW SCHEMAS IN `data_engineering-uc-dev`"
        self.assert_sql_equal(result, expected_sql)

    def test_list_schemas_sql_without_database(self, template_bundle):
        result = self.run_macro(template_bundle.template, "list_schemas_sql", None)

        expected_sql = "SHOW SCHEMAS"
        self.assert_sql_equal(result, expected_sql)

    def test_check_schema_exists_sql_with_hyphenated_database(self, template_bundle):
        result = self.run_macro(
            template_bundle.template,
            "check_schema_exists_sql",
            "data_engineering-uc-dev",
            "my_schema",
        )

        expected_sql = "SHOW SCHEMAS IN `data_engineering-uc-dev` LIKE 'my_schema'"
        self.assert_sql_equal(result, expected_sql)

    def test_case_sensitivity(self, template_bundle):
        relation = Mock()
        relation.database = "TEST_DB"
        relation.schema = "TEST_SCHEMA"
        relation.identifier = "TEST_TABLE"
        relation.render = Mock(return_value="`TEST_DB`.`TEST_SCHEMA`.`TEST_TABLE`")

        result = self.run_macro(template_bundle.template, "get_uc_tables_sql", relation)

        expected_sql = """
            SELECT
              table_name,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), 'table', lower(table_type)) AS table_type,
              lower(data_source_format) AS file_format,
              table_owner,
              if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), lower(table_type), null) AS databricks_table_type
            FROM `system`.`information_schema`.`tables`
            WHERE table_catalog = 'test_db'
              AND table_schema = 'test_schema'
              AND table_name = 'test_table'
        """  # noqa
        self.assert_sql_equal(result, expected_sql)
