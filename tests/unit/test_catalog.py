from unittest.mock import Mock, patch

import pytest

from dbt.adapters.databricks.behaviors.catalog import (
    DefaultGetCatalogBehavior,
    get_identifier_list_string,
)
from dbt.adapters.databricks.relation import DatabricksRelation


class TestIdentifierString:
    def test_describe_table_extended_2048_char_limit(self):
        """GIVEN a list of table_names whos total character length exceeds 2048 characters
        WHEN the environment variable DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS is "true"
        THEN the identifier list is replaced with "*"
        """

        table_names = set([f"customers_{i}" for i in range(200)])

        # By default, don't limit the number of characters
        assert get_identifier_list_string(table_names) == "|".join(table_names)

        # If environment variable is set, then limit the number of characters
        with patch(
            "dbt.adapters.databricks.global_state.GlobalState.get_char_limit_bypass",
            return_value="true",
        ):
            # Long list of table names is capped
            assert get_identifier_list_string(table_names) == "*"

            # Short list of table names is not capped
            assert get_identifier_list_string(list(table_names)[:5]) == "|".join(
                list(table_names)[:5]
            )

    def test_describe_table_extended_should_not_limit(self):
        """GIVEN a list of table_names whos total character length exceeds 2048 characters
        WHEN the environment variable DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS is not set
        THEN the identifier list is not truncated
        """

        table_names = set([f"customers_{i}" for i in range(200)])

        # By default, don't limit the number of characters
        assert get_identifier_list_string(table_names) == "|".join(table_names)

    def test_describe_table_extended_should_limit(self):
        """GIVEN a list of table_names whos total character length exceeds 2048 characters
        WHEN the environment variable DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS is "true"
        THEN the identifier list is replaced with "*"
        """

        table_names = set([f"customers_{i}" for i in range(200)])

        # If environment variable is set, then limit the number of characters
        with patch(
            "dbt.adapters.databricks.global_state.GlobalState.get_char_limit_bypass",
            return_value="true",
        ):
            # Long list of table names is capped
            assert get_identifier_list_string(table_names) == "*"

    def test_describe_table_extended_may_limit(self):
        """GIVEN a list of table_names whos total character length does not 2048 characters
        WHEN the environment variable DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS is "true"
        THEN the identifier list is not truncated
        """

        table_names = set([f"customers_{i}" for i in range(200)])

        # If environment variable is set, then we may limit the number of characters
        with patch(
            "dbt.adapters.databricks.global_state.GlobalState.get_char_limit_bypass",
            return_value="true",
        ):
            # But a short list of table names is not capped
            assert get_identifier_list_string(list(table_names)[:5]) == "|".join(
                list(table_names)[:5]
            )


class TestDefaultGetCatalogBehavior:
    @pytest.fixture
    def behavior(self):
        return DefaultGetCatalogBehavior()

    def test_get_schema_for_catalog__no_columns(self, behavior):
        with patch.object(
            DefaultGetCatalogBehavior, "_list_relations_with_information"
        ) as list_info:
            list_info.return_value = [(Mock(), "info")]
            with patch.object(DefaultGetCatalogBehavior, "_get_columns_for_catalog") as get_columns:
                get_columns.return_value = []
                table = behavior._get_schema_for_catalog(Mock(), "database", "schema", "name")
                assert len(table.rows) == 0

    def test_get_schema_for_catalog__some_columns(self, behavior):
        with patch.object(
            DefaultGetCatalogBehavior, "_list_relations_with_information"
        ) as list_info:
            list_info.return_value = [(Mock(), "info")]
            with patch.object(DefaultGetCatalogBehavior, "_get_columns_for_catalog") as get_columns:
                get_columns.return_value = [
                    {"name": "col1", "type": "string", "comment": "comment"},
                    {"name": "col2", "type": "string", "comment": "comment"},
                ]
                table = behavior._get_schema_for_catalog(Mock(), "database", "schema", "name")
                assert len(table.rows) == 2
                assert table.column_names == ("name", "type", "comment")

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self, behavior):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        # Mimics the output of Spark in the information column
        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: delta\n"
            "Statistics: 123456789 bytes\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Partition Provider: Catalog\n"
            "Partition Columns: [`dt`]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )

        columns = behavior.parse_columns_from_information(relation, information)
        assert len(columns) == 4
        assert columns[0].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "col1",
            "column_index": 0,
            "dtype": "decimal(22,0)",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 123456789,
            "comment": None,
        }

        assert columns[3].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "struct_col",
            "column_index": 3,
            "dtype": "struct",
            "comment": None,
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 123456789,
        }

    def test_parse_columns_from_information_with_view_type(self, behavior):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.View
        information = (
            "Database: default_schema\n"
            "Table: myview\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: UNKNOWN\n"
            "Created By: Spark 3.0.1\n"
            "Type: VIEW\n"
            "View Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Original Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Catalog and Namespace: spark_catalog.default\n"
            "View Query Output Columns: [col1, col2, dt]\n"
            "Table Properties: [view.query.out.col.1=col1, view.query.out.col.2=col2, "
            "transient_lastDdlTime=1618324324, view.query.out.col.3=dt, "
            "view.catalogAndNamespace.part.0=spark_catalog, "
            "view.catalogAndNamespace.part.1=default]\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Storage Properties: [serialization.format=1]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = DatabricksRelation.create(
            schema="default_schema", identifier="myview", type=rel_type
        )

        columns = behavior.parse_columns_from_information(relation, information)
        assert len(columns) == 4
        assert columns[1].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "col2",
            "column_index": 1,
            "comment": None,
            "dtype": "string",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
        }

        assert columns[3].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "struct_col",
            "column_index": 3,
            "comment": None,
            "dtype": "struct",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
        }

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self, behavior):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: parquet\n"
            "Statistics: 1234567890 bytes, 12345678 rows\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n"
            "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )

        columns = behavior.parse_columns_from_information(relation, information)
        assert len(columns) == 4
        assert columns[2].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "dt",
            "column_index": 2,
            "comment": None,
            "dtype": "date",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 1234567890,
            "stats:rows:description": "",
            "stats:rows:include": True,
            "stats:rows:label": "rows",
            "stats:rows:value": 12345678,
        }

        assert columns[3].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "struct_col",
            "column_index": 3,
            "comment": None,
            "dtype": "struct",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 1234567890,
            "stats:rows:description": "",
            "stats:rows:include": True,
            "stats:rows:label": "rows",
            "stats:rows:value": 12345678,
        }
