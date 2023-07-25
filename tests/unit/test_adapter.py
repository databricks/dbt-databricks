import unittest
from unittest import mock

from agate import Row
import dbt.flags as flags
import dbt.exceptions

from dbt.adapters.databricks import __version__
from dbt.adapters.databricks import DatabricksAdapter, DatabricksRelation
from dbt.adapters.databricks.impl import check_not_found_error
from dbt.adapters.databricks.connections import (
    CATALOG_KEY_IN_SESSION_PROPERTIES,
    DBT_DATABRICKS_INVOCATION_ENV,
    DBT_DATABRICKS_HTTP_SESSION_HEADERS,
)
from tests.unit.utils import config_from_parts_or_dicts


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

    def _get_target_databricks_sql_connector_no_token(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "databricks",
                        "schema": "analytics",
                        "host": "yourorg.databricks.com",
                        "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                        "session_properties": {"spark.sql.ansi.enabled": "true"},
                    }
                },
                "target": "test",
            },
        )

    def _get_target_databricks_sql_connector_client_creds(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "databricks",
                        "schema": "analytics",
                        "host": "yourorg.databricks.com",
                        "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                        "client_id": "foo",
                        "client_secret": "bar",
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

    def _get_target_databricks_sql_connector_http_header(self, project, http_header):
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
                        "connection_parameters": {"http_headers": http_header},
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

    def test_reserved_connection_parameters(self):
        with self.assertRaisesRegex(
            dbt.exceptions.DbtProfileError,
            "The connection parameter `server_hostname` is reserved.",
        ):
            config_from_parts_or_dicts(
                self.project_cfg,
                {
                    "outputs": {
                        "test": {
                            "type": "databricks",
                            "schema": "analytics",
                            "host": "yourorg.databricks.com",
                            "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                            "token": "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                            "connection_parameters": {"server_hostname": "theirorg.databricks.com"},
                        }
                    },
                    "target": "test",
                },
            )

    def test_invalid_http_headers(self):
        def test_http_headers(http_header):
            with self.assertRaisesRegex(
                dbt.exceptions.DbtProfileError,
                "The connection parameter `http_headers` should be dict of strings.",
            ):
                self._get_target_databricks_sql_connector_http_header(self.project_cfg, http_header)

        test_http_headers("a")
        test_http_headers(["a", "b"])
        test_http_headers({"a": 1, "b": 2})

    def test_invalid_custom_user_agent(self):
        with self.assertRaisesRegex(
            dbt.exceptions.DbtValidationError,
            "Invalid invocation environment",
        ):
            config = self._get_target_databricks_sql_connector(self.project_cfg)
            adapter = DatabricksAdapter(config)
            with mock.patch.dict("os.environ", **{DBT_DATABRICKS_INVOCATION_ENV: "(Some-thing)"}):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

    def test_custom_user_agent(self):
        config = self._get_target_databricks_sql_connector(self.project_cfg)
        adapter = DatabricksAdapter(config)

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
            new=self._connect_func(expected_invocation_env="databricks-workflows"),
        ):
            with mock.patch.dict(
                "os.environ", **{DBT_DATABRICKS_INVOCATION_ENV: "databricks-workflows"}
            ):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

    def test_environment_single_http_header(self):
        self._test_environment_http_headers(
            http_headers_str='{"test":{"jobId":1,"runId":12123}}',
            expected_http_headers=[("test", '{"jobId": 1, "runId": 12123}')],
        )

    def test_environment_multiple_http_headers(self):
        self._test_environment_http_headers(
            http_headers_str='{"test":{"jobId":1,"runId":12123},"dummy":{"jobId":1,"runId":12123}}',
            expected_http_headers=[
                ("test", '{"jobId": 1, "runId": 12123}'),
                ("dummy", '{"jobId": 1, "runId": 12123}'),
            ],
        )

    def test_environment_users_http_headers_intersection_error(self):
        with self.assertRaisesRegex(
            dbt.exceptions.DbtValidationError,
            r"Intersection with reserved http_headers in keys: {'t'}",
        ):
            self._test_environment_http_headers(
                http_headers_str='{"t":{"jobId":1,"runId":12123},"d":{"jobId":1,"runId":12123}}',
                expected_http_headers=[],
                user_http_headers={"t": "test", "nothing": "nothing"},
            )

    def test_environment_users_http_headers_union_success(self):
        self._test_environment_http_headers(
            http_headers_str='{"t":{"jobId":1,"runId":12123},"d":{"jobId":1,"runId":12123}}',
            user_http_headers={"nothing": "nothing"},
            expected_http_headers=[
                ("t", '{"jobId": 1, "runId": 12123}'),
                ("d", '{"jobId": 1, "runId": 12123}'),
                ("nothing", "nothing"),
            ],
        )

    def test_environment_http_headers_string(self):
        self._test_environment_http_headers(
            http_headers_str='{"string":"some-string"}',
            expected_http_headers=[("string", "some-string")],
        )

    def _test_environment_http_headers(
        self, http_headers_str, expected_http_headers, user_http_headers=None
    ):
        if user_http_headers:
            config = self._get_target_databricks_sql_connector_http_header(
                self.project_cfg, user_http_headers
            )
        else:
            config = self._get_target_databricks_sql_connector(self.project_cfg)

        adapter = DatabricksAdapter(config)

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
            new=self._connect_func(expected_http_headers=expected_http_headers),
        ):
            with mock.patch.dict(
                "os.environ",
                **{DBT_DATABRICKS_HTTP_SESSION_HEADERS: http_headers_str},
            ):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

    @unittest.skip("not ready")
    def test_oauth_settings(self):
        config = self._get_target_databricks_sql_connector_no_token(self.project_cfg)

        adapter = DatabricksAdapter(config)

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
            new=self._connect_func(expected_no_token=True),
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

    @unittest.skip("not ready")
    def test_client_creds_settings(self):
        config = self._get_target_databricks_sql_connector_client_creds(self.project_cfg)

        adapter = DatabricksAdapter(config)

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
            new=self._connect_func(expected_client_creds=True),
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

    def _connect_func(
        self,
        *,
        expected_catalog=None,
        expected_invocation_env=None,
        expected_http_headers=None,
        expected_no_token=None,
        expected_client_creds=None,
    ):
        def connect(
            server_hostname,
            http_path,
            credentials_provider,
            http_headers,
            session_configuration,
            catalog,
            _user_agent_entry,
            **kwargs,
        ):
            self.assertEqual(server_hostname, "yourorg.databricks.com")
            self.assertEqual(http_path, "sql/protocolv1/o/1234567890123456/1234-567890-test123")
            if not (expected_no_token or expected_client_creds):
                self.assertEqual(
                    credentials_provider._token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                )
            if expected_client_creds:
                self.assertEqual(kwargs.get("client_id"), "foo")
                self.assertEqual(kwargs.get("client_secret"), "bar")
            self.assertEqual(session_configuration["spark.sql.ansi.enabled"], "true")
            if expected_catalog is None:
                self.assertIsNone(catalog)
            else:
                self.assertEqual(catalog, expected_catalog)
            if expected_invocation_env is not None:
                self.assertEqual(
                    _user_agent_entry,
                    f"dbt-databricks/{__version__.version}; {expected_invocation_env}",
                )
            else:
                self.assertEqual(_user_agent_entry, f"dbt-databricks/{__version__.version}")
            if expected_http_headers is None:
                self.assertIsNone(http_headers)
            else:
                self.assertEqual(http_headers, expected_http_headers)

        return connect

    def test_databricks_sql_connector_connection(self):
        self._test_databricks_sql_connector_connection(self._connect_func())

    def _test_databricks_sql_connector_connection(self, connect):
        config = self._get_target_databricks_sql_connector(self.project_cfg)
        adapter = DatabricksAdapter(config)

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(
                connection.credentials.http_path,
                "sql/protocolv1/o/1234567890123456/1234-567890-test123",
            )
            self.assertEqual(connection.credentials.token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertEqual(len(connection.credentials.session_properties), 1)
            self.assertEqual(
                connection.credentials.session_properties["spark.sql.ansi.enabled"],
                "true",
            )
            self.assertIsNone(connection.credentials.database)

    def test_databricks_sql_connector_catalog_connection(self):
        self._test_databricks_sql_connector_catalog_connection(
            self._connect_func(expected_catalog="main")
        )

    def _test_databricks_sql_connector_catalog_connection(self, connect):
        config = self._get_target_databricks_sql_connector_catalog(self.project_cfg)
        adapter = DatabricksAdapter(config)

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(
                connection.credentials.http_path,
                "sql/protocolv1/o/1234567890123456/1234-567890-test123",
            )
            self.assertEqual(connection.credentials.token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertEqual(connection.credentials.database, "main")

    def test_databricks_sql_connector_http_header_connection(self):
        self._test_databricks_sql_connector_http_header_connection(
            {"aaa": "xxx"}, self._connect_func(expected_http_headers=[("aaa", "xxx")])
        )
        self._test_databricks_sql_connector_http_header_connection(
            {"aaa": "xxx", "bbb": "yyy"},
            self._connect_func(expected_http_headers=[("aaa", "xxx"), ("bbb", "yyy")]),
        )

    def _test_databricks_sql_connector_http_header_connection(self, http_headers, connect):
        config = self._get_target_databricks_sql_connector_http_header(
            self.project_cfg, http_headers
        )
        adapter = DatabricksAdapter(config)

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(
                connection.credentials.http_path,
                "sql/protocolv1/o/1234567890123456/1234-567890-test123",
            )
            self.assertEqual(connection.credentials.token, "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_simple_catalog_relation(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            database="test_catalog",
            schema="default_schema",
            identifier="mytable",
            type=rel_type,
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
            (
                "OutputFormat",
                "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            ),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        metadata, rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)

        self.assertDictEqual(
            metadata,
            {
                "# col_name": "data_type",
                "dt": "date",
                None: None,
                "# Detailed Table Information": None,
                "Database": None,
                "Owner": "root",
                "Created Time": "Wed Feb 04 18:15:00 UTC 1815",
                "Last Access": "Wed May 20 19:25:00 UTC 1925",
                "Type": "MANAGED",
                "Provider": "delta",
                "Location": "/mnt/vo",
                "Serde Library": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                "Partition Provider": "Catalog",
            },
        )

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
                "column_index": 1,
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
                "column_index": 2,
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
                "column_index": 3,
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
                "column_index": 4,
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
        _, rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)

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
            (
                "OutputFormat",
                "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            ),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        metadata, rows = DatabricksAdapter(config).parse_describe_extended(relation, input_cols)

        self.assertEqual(
            metadata,
            {
                None: None,
                "# Detailed Table Information": None,
                "Database": None,
                "Owner": "root",
                "Created Time": "Wed Feb 04 18:15:00 UTC 1815",
                "Last Access": "Wed May 20 19:25:00 UTC 1925",
                "Statistics": "1109049927 bytes, 14093476 rows",
                "Type": "MANAGED",
                "Provider": "delta",
                "Location": "/mnt/vo",
                "Serde Library": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                "Partition Provider": "Catalog",
            },
        )

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
                "column_index": 1,
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
            schema="default_schema", identifier="mytable", type=rel_type
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation, information)
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
                "column_index": 1,
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
                "column_index": 4,
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
            schema="default_schema", identifier="myview", type=rel_type
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation, information)
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
                "column_index": 2,
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
                "column_index": 4,
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
            schema="default_schema", identifier="mytable", type=rel_type
        )

        config = self._get_target_databricks_sql_connector(self.project_cfg)
        columns = DatabricksAdapter(config).parse_columns_from_information(relation, information)
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
                "column_index": 3,
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
                "column_index": 4,
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


class TestCheckNotFound(unittest.TestCase):
    def test_prefix(self):
        self.assertTrue(check_not_found_error("Runtime error \n Database 'dbt' not found"))

    def test_no_prefix_or_suffix(self):
        self.assertTrue(check_not_found_error("Database not found"))

    def test_quotes(self):
        self.assertTrue(check_not_found_error("Database '`dbt`' not found"))

    def test_suffix(self):
        self.assertTrue(check_not_found_error("Database not found and \n foo"))

    def test_error_condition(self):
        self.assertTrue(check_not_found_error("[SCHEMA_NOT_FOUND]"))

    def test_unexpected_error(self):
        self.assertFalse(check_not_found_error("[DATABASE_NOT_FOUND]"))
        self.assertFalse(check_not_found_error("Schema foo not found"))
        self.assertFalse(check_not_found_error("Database 'foo' not there"))
