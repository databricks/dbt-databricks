from unittest.mock import MagicMock, Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestCatalogMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters/catalog.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/adapters"]

    def test_get_catalog_tables_sql(self, template_bundle):
        """Test the SQL generation for catalog tables query"""
        information_schema = {"database": "test_catalog"}

        result = self.run_macro(
            template_bundle.template,
            "databricks__get_catalog_tables_sql",
            information_schema,
        )

        expected = """
            select
                table_catalog as table_database,
                table_schema,
                table_name,
                lower(table_type) as table_type,
                comment as table_comment,
                table_owner,
                'Last Modified' as `stats:last_modified:label`,
                last_altered as `stats:last_modified:value`,
                'The timestamp for last update/change' as `stats:last_modified:description`,
                (last_altered is not null and table_type not ilike '%VIEW%') as
                   `stats:last_modified:include`
            from `system`.`information_schema`.`tables`
        """

        self.assert_sql_equal(result, expected)

    def test_get_catalog_columns_sql(self, template_bundle):
        """Test the SQL generation for catalog columns query"""
        information_schema = {"database": "test_catalog"}

        result = self.run_macro(
            template_bundle.template,
            "databricks__get_catalog_columns_sql",
            information_schema,
        )

        expected = """
            select
                table_catalog as table_database,
                table_schema,
                table_name,
                column_name,
                ordinal_position as column_index,
                lower(full_data_type) as column_type,
                comment as column_comment
            from `system`.`information_schema`.`columns`
        """

        self.assert_sql_equal(result, expected)

    def test_get_catalog_results_sql(self, template_bundle):
        """Test the SQL generation for joining tables and columns"""
        result = self.run_macro(template_bundle.template, "databricks__get_catalog_results_sql")

        expected = """
            select *
            from tables
            join columns using (table_database, table_schema, table_name)
            order by column_index
        """

        self.assert_sql_equal(result, expected)

    def test_get_catalog_schemas_where_clause_sql(self, template_bundle):
        """Test the SQL generation for schemas where clause"""
        catalog = "test_catalog"
        schemas = [["catalog_name", "schema1"], ["catalog_name", "schema2"]]

        result = self.run_macro(
            template_bundle.template,
            "databricks__get_catalog_schemas_where_clause_sql",
            catalog,
            schemas,
        )

        expected = """
            where table_catalog = 'test_catalog'
            and (
                table_schema = 'schema1' or
                table_schema = 'schema2'
            )
        """

        self.assert_sql_equal(result, expected)

    def test_get_catalog_relations_where_clause_sql(self, template_bundle):
        """Test the SQL generation for relations where clause"""
        # Create mock relations
        relation1 = MagicMock()
        relation1.schema = "test_schema1"
        relation1.identifier = "test_table1"

        relation2 = MagicMock()
        relation2.schema = "test_schema2"
        relation2.identifier = "test_table2"

        relations = [relation1, relation2]
        catalog = "test_catalog"

        result = self.run_macro(
            template_bundle.template,
            "databricks__get_catalog_relations_where_clause_sql",
            catalog,
            relations,
        )

        expected = """
            where table_catalog = 'test_catalog'
            and (
                (table_schema = 'test_schema1' and table_name = 'test_table1') or
                (table_schema = 'test_schema2' and table_name = 'test_table2')
            )
        """

        self.assert_sql_equal(result, expected)

    def test_get_catalog(self, template_bundle, context):
        """Test the full get_catalog macro with mocked run_query"""
        information_schema = {"database": "test_catalog"}
        schemas = [["catalog_name", "schema1"], ["catalog_name", "schema2"]]

        # Mock run_query to return a dummy result
        mock_result = Mock()
        context["run_query_as"] = Mock(return_value=mock_result)

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_catalog",
            information_schema,
            schemas,
        )

        # Verify run_query was called once and capture the SQL
        context["run_query_as"].assert_called_once()
        sql_query = context["run_query_as"].call_args[0][0]

        # Check complete SQL structure
        expected_sql = """
            with tables as (
                select
                    table_catalog as table_database,
                    table_schema,
                    table_name,
                    lower(table_type) as table_type,
                    comment as table_comment,
                    table_owner,
                    'Last Modified' as `stats:last_modified:label`,
                    last_altered as `stats:last_modified:value`,
                    'The timestamp for last update/change' as `stats:last_modified:description`,
                    (last_altered is not null and table_type not ilike '%VIEW%') as
                    `stats:last_modified:include`
                from `system`.`information_schema`.`tables`
                where table_catalog = 'test_catalog'
                and (
                    table_schema = 'schema1' or
                    table_schema = 'schema2'
                )
            ),

            columns as (
                select
                    table_catalog as table_database,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position as column_index,
                    lower(full_data_type) as column_type,
                    comment as column_comment
                from `system`.`information_schema`.`columns`
                where table_catalog = 'test_catalog'
                and (
                    table_schema = 'schema1' or
                    table_schema = 'schema2'
                )
            )

            select *
            from tables
            join columns using (table_database, table_schema, table_name)
            order by column_index
        """

        self.assert_sql_equal(sql_query, expected_sql)

    def test_get_catalog_relations(self, template_bundle, context):
        """Test the get_catalog_relations macro with mocked run_query"""
        information_schema = {"database": "test_catalog"}

        relation1 = MagicMock()
        relation1.schema = "test_schema1"
        relation1.identifier = "test_table1"

        relation2 = MagicMock()
        relation2.schema = "test_schema2"
        relation2.identifier = "test_table2"

        relations = [relation1, relation2]

        mock_result = Mock()
        context["run_query_as"] = Mock(return_value=mock_result)

        self.run_macro_raw(
            template_bundle.template,
            "databricks__get_catalog_relations",
            information_schema,
            relations,
        )

        sql_query = context["run_query_as"].call_args[0][0]

        # Check complete SQL structure
        expected_sql = """
            with tables as (
                select
                    table_catalog as table_database,
                    table_schema,
                    table_name,
                    lower(table_type) as table_type,
                    comment as table_comment,
                    table_owner,
                    'Last Modified' as `stats:last_modified:label`,
                    last_altered as `stats:last_modified:value`,
                    'The timestamp for last update/change' as `stats:last_modified:description`,
                    (last_altered is not null and table_type not ilike '%VIEW%') as
                    `stats:last_modified:include`
                from `system`.`information_schema`.`tables`
                where table_catalog = 'test_catalog'
                and (
                    (table_schema = 'test_schema1' and table_name = 'test_table1') or
                    (table_schema = 'test_schema2' and table_name = 'test_table2')
                )
            ),

            columns as (
                select
                    table_catalog as table_database,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position as column_index,
                    lower(full_data_type) as column_type,
                    comment as column_comment
                from `system`.`information_schema`.`columns`
                where table_catalog = 'test_catalog'
                and (
                    (table_schema = 'test_schema1' and table_name = 'test_table1') or
                    (table_schema = 'test_schema2' and table_name = 'test_table2')
                )
            )

            select *
            from tables
            join columns using (table_database, table_schema, table_name)
            order by column_index
        """

        self.assert_sql_equal(sql_query, expected_sql)
