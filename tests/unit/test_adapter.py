import unittest
from unittest import mock

import dbt.exceptions
from agate import Row
import dbt.flags as flags

from dbt.adapters.databricks import __version__
from dbt.adapters.databricks import DatabricksAdapter, DatabricksRelation
from dbt.adapters.databricks.connections import CATALOG_KEY_IN_SESSION_PROPERTIES
from .utils import config_from_parts_or_dicts


class TestDatabricksAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

    def _get_target_databricks_sql_connector(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "databricks",
                        "schema": "analytics",
                        "host": "yourorg.databricks.com",
                        "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                        "token": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                        "session_properties": {"spark.sql.ansi.enabled": "true"},
                    }
                },
                "target": "test",
            },
        )

    def _get_target_databricks_sql_connector_catalog(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "databricks",
                        "schema": "analytics",
                        "catalog": "main",
                        "host": "yourorg.databricks.com",
                        "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                        "token": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                        "session_properties": {"spark.sql.ansi.enabled": "true"},
                    }
                },
                "target": "test",
            },
        )

    def test_two_catalog_settings(self):
        with self.assertRaisesRegex(
            dbt.exceptions.DbtProfileError,
            "Got duplicate keys: \\(`databricks.catalog` in session_properties\\)"
            ' all map to "database"',
        ):
            config_from_parts_or_dicts(
                self.project_cfg,
                {
                    "outputs": {
                        "test": {
                            "type": "databricks",
                            "schema": "analytics",
                            "catalog": "main",
                            "host": "yourorg.databricks.com",
                            "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                            "token": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                            "session_properties": {
                                CATALOG_KEY_IN_SESSION_PROPERTIES: "catalog",
                                "spark.sql.ansi.enabled": "true",
                            },
                        }
                    },
                    "target": "test",
                },
            )

    def test_database_and_catalog_settings(self):
        with self.assertRaisesRegex(
            dbt.exceptions.DbtProfileError,
            'Got duplicate keys: \\(catalog\\) all map to "database"',
        ):
            config_from_parts_or_dicts(
                self.project_cfg,
                {
                    "outputs": {
                        "test": {
                            "type": "databricks",
                            "schema": "analytics",
                            "catalog": "main",
                            "database": "database",
                            "host": "yourorg.databricks.com",
                            "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                            "token": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                            "session_properties": {"spark.sql.ansi.enabled": "true"},
                        }
                    },
                    "target": "test",
                },
            )

    def test_databricks_sql_connector_connection_lt_2_0(self):
        def connect(
            server_hostname, http_path, access_token, session_configuration, _user_agent_entry
        ):
            self.assertEqual(server_hostname, "yourorg.databricks.com")
            self.assertEqual(http_path, "sql/protocolv1/o/1234567890123456/1234-567890-test123")
            self.assertEqual(access_token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(session_configuration["spark.sql.ansi.enabled"], "true")
            self.assertNotIn(CATALOG_KEY_IN_SESSION_PROPERTIES, session_configuration)
            self.assertEqual(_user_agent_entry, f"dbt-databricks/{__version__.version}")

        self._test_databricks_sql_connector_connection("1.0.2", connect)

    def test_databricks_sql_connector_connection_ge_2_0(self):
        def connect(
            server_hostname,
            http_path,
            access_token,
            session_configuration,
            catalog,
            _user_agent_entry,
        ):
            self.assertEqual(server_hostname, "yourorg.databricks.com")
            self.assertEqual(http_path, "sql/protocolv1/o/1234567890123456/1234-567890-test123")
            self.assertEqual(access_token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(session_configuration["spark.sql.ansi.enabled"], "true")
            self.assertIsNone(catalog)
            self.assertEqual(_user_agent_entry, f"dbt-databricks/{__version__.version}")

        self._test_databricks_sql_connector_connection("2.0.0", connect)

    def _test_databricks_sql_connector_connection(self, version, connect):
        config = self._get_target_databricks_sql_connector(self.project_cfg)
        adapter = DatabricksAdapter(config)

        with mock.patch("dbt.adapters.databricks.connections.dbsql.__version__", new=version):
            with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

                self.assertEqual(connection.state, "open")
                self.assertIsNotNone(connection.handle)
                self.assertEqual(
                    connection.credentials.http_path,
                    "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                )
                self.assertEqual(
                    connection.credentials.token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                )
                self.assertEqual(connection.credentials.schema, "analytics")
                self.assertEqual(len(connection.credentials.session_properties), 1)
                self.assertEqual(
                    connection.credentials.session_properties["spark.sql.ansi.enabled"], "true"
                )
                self.assertIsNone(connection.credentials.database)

    def test_databricks_sql_connector_catalog_connection_lt_2_0(self):
        def connect(
            server_hostname, http_path, access_token, session_configuration, _user_agent_entry
        ):
            self.assertEqual(server_hostname, "yourorg.databricks.com")
            self.assertEqual(http_path, "sql/protocolv1/o/1234567890123456/1234-567890-test123")
            self.assertEqual(access_token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(session_configuration["spark.sql.ansi.enabled"], "true")
            self.assertEqual(session_configuration[CATALOG_KEY_IN_SESSION_PROPERTIES], "main")
            self.assertEqual(_user_agent_entry, f"dbt-databricks/{__version__.version}")

        self._test_databricks_sql_connector_catalog_connection("1.0.2", connect)

    def test_databricks_sql_connector_catalog_connection_ge_2_0(self):
        def connect(
            server_hostname,
            http_path,
            access_token,
            session_configuration,
            catalog,
            _user_agent_entry,
        ):
            self.assertEqual(server_hostname, "yourorg.databricks.com")
            self.assertEqual(http_path, "sql/protocolv1/o/1234567890123456/1234-567890-test123")
            self.assertEqual(access_token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(session_configuration["spark.sql.ansi.enabled"], "true")
            self.assertEqual(catalog, "main")
            self.assertEqual(_user_agent_entry, f"dbt-databricks/{__version__.version}")

        self._test_databricks_sql_connector_catalog_connection("2.0.0", connect)

    def _test_databricks_sql_connector_catalog_connection(self, version, connect):
        config = self._get_target_databricks_sql_connector_catalog(self.project_cfg)
        adapter = DatabricksAdapter(config)

        with mock.patch("dbt.adapters.databricks.connections.dbsql.__version__", new=version):
            with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

                self.assertEqual(connection.state, "open")
                self.assertIsNotNone(connection.handle)
                self.assertEqual(
                    connection.credentials.http_path,
                    "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                )
                self.assertEqual(
                    connection.credentials.token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                )
                self.assertEqual(connection.credentials.schema, "analytics")
                self.assertEqual(connection.credentials.database, "main")

    def test_simple_catalog_relation(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            database="test_catalog", schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database == "test_catalog"

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            (
                "col2",
                "string",
            ),
            ("dt", "date"),
            ("struct_col", "struct<struct_inner_col:string>"),
            ("# Partition Information", "data_type"),
            ("# col_name", "data_type"),
            ("dt", "date"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Detailed Table Information", None),
            ("Owner", 1234),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)

        self.assertEqual(rows[0].to_column_dict().get("table_owner"), "1234")

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Partition Information", "data_type"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Statistics", "1109049927 bytes, 14093476 rows"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            },
        )

    def test_relation_with_database(self):
        config = self._get_target_databricks_sql_connector_catalog(self.project_cfg)
        adapter = DatabricksAdapter(config)
        r1 = adapter.Relation.create(schema="different", identifier="table")
        assert r1.database is None
        r2 = adapter.Relation.create(database="something", schema="different", identifier="table")
        assert r2.database == "something"

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self):
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
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
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
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_parse_columns_from_information_with_view_type(self):
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
            schema="default_schema", identifier="myview", type=rel_type, information=information
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self):
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
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
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
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
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
            },
        )
