import re
from multiprocessing import get_context
from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest
from agate import Row
from dbt_common.exceptions import DbtConfigError, DbtValidationError

import dbt.flags as flags
from dbt.adapters.databricks import DatabricksAdapter, __version__
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.credentials import (
    CATALOG_KEY_IN_SESSION_PROPERTIES,
)
from dbt.adapters.databricks.impl import (
    DatabricksRelationInfo,
    get_identifier_list_string,
)
from dbt.adapters.databricks.relation import (
    DatabricksRelation,
    DatabricksRelationType,
    DatabricksTableType,
)
from dbt.adapters.databricks.utils import check_not_found_error
from dbt.config import RuntimeConfig
from tests.unit.utils import config_from_parts_or_dicts


class DatabricksAdapterBase:
    @pytest.fixture(autouse=True)
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

        self.profile_cfg = {
            "outputs": {
                "test": {
                    "type": "databricks",
                    "catalog": "main",
                    "schema": "analytics",
                    "host": "yourorg.databricks.com",
                    "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                }
            },
            "target": "test",
        }

    def _get_config(
        self,
        token: Optional[str] = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        session_properties: Optional[dict[str, str]] = {"spark.sql.ansi.enabled": "true"},
        **kwargs: Any,
    ) -> RuntimeConfig:
        if token:
            self.profile_cfg["outputs"]["test"]["token"] = token
        if session_properties:
            self.profile_cfg["outputs"]["test"]["session_properties"] = session_properties

        for key, val in kwargs.items():
            self.profile_cfg["outputs"]["test"][key] = val

        return config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)


class TestDatabricksAdapter(DatabricksAdapterBase):
    def test_two_catalog_settings(self):
        with pytest.raises(DbtConfigError) as excinfo:
            self._get_config(
                session_properties={
                    CATALOG_KEY_IN_SESSION_PROPERTIES: "catalog",
                    "spark.sql.ansi.enabled": "true",
                }
            )

        expected_message = (
            'Got duplicate keys: (`databricks.catalog` in session_properties) all map to "database"'
        )

        assert expected_message in str(excinfo.value)

    def test_database_and_catalog_settings(self):
        with pytest.raises(DbtConfigError) as excinfo:
            self._get_config(catalog="main", database="database")

        assert 'Got duplicate keys: (catalog) all map to "database"' in str(excinfo.value)

    def test_reserved_connection_parameters(self):
        with pytest.raises(DbtConfigError) as excinfo:
            self._get_config(connection_parameters={"server_hostname": "theirorg.databricks.com"})

        assert "The connection parameter `server_hostname` is reserved." in str(excinfo.value)

    def test_invalid_http_headers(self):
        def test_http_headers(http_header):
            with pytest.raises(DbtConfigError) as excinfo:
                self._get_config(connection_parameters={"http_headers": http_header})

            assert "The connection parameter `http_headers` should be dict of strings" in str(
                excinfo.value
            )

        test_http_headers("a")
        test_http_headers(["a", "b"])
        test_http_headers({"a": 1, "b": 2})

    def test_invalid_custom_user_agent(self):
        with pytest.raises(DbtValidationError) as excinfo:
            config = self._get_config()
            adapter = DatabricksAdapter(config, get_context("spawn"))
            with patch(
                "dbt.adapters.databricks.global_state.GlobalState.get_invocation_env",
                return_value="(Some-thing)",
            ):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

        assert "Invalid invocation environment" in str(excinfo.value)

    def test_custom_user_agent(self):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch(
            "dbt.adapters.databricks.handle.dbsql.connect",
            new=self._connect_func(expected_invocation_env="databricks-workflows"),
        ):
            with patch(
                "dbt.adapters.databricks.global_state.GlobalState.get_invocation_env",
                return_value="databricks-workflows",
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
        with pytest.raises(DbtValidationError) as excinfo:
            self._test_environment_http_headers(
                http_headers_str='{"t":{"jobId":1,"runId":12123},"d":{"jobId":1,"runId":12123}}',
                expected_http_headers=[],
                user_http_headers={"t": "test", "nothing": "nothing"},
            )

        assert "Intersection with reserved http_headers in keys: {'t'}" in str(excinfo.value)

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
            config = self._get_config(connection_parameters={"http_headers": user_http_headers})
        else:
            config = self._get_config()

        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch(
            "dbt.adapters.databricks.handle.dbsql.connect",
            new=self._connect_func(expected_http_headers=expected_http_headers),
        ):
            with patch(
                "dbt.adapters.databricks.global_state.GlobalState.get_http_session_headers",
                return_value=http_headers_str,
            ):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

    @pytest.mark.skip("not ready")
    def test_oauth_settings(self):
        config = self._get_config(token=None)

        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch(
            "dbt.adapters.databricks.handle.dbsql.connect",
            new=self._connect_func(expected_no_token=True),
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

    @pytest.mark.skip("not ready")
    def test_client_creds_settings(self):
        config = self._get_config(client_id="foo", client_secret="bar")

        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch(
            "dbt.adapters.databricks.handle.dbsql.connect",
            new=self._connect_func(expected_client_creds=True),
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

    def _connect_func(
        self,
        *,
        expected_catalog="main",
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
            assert server_hostname == "yourorg.databricks.com"
            assert http_path == "sql/protocolv1/o/1234567890123456/1234-567890-test123"

            if not (expected_no_token or expected_client_creds):
                assert (
                    credentials_provider()().get("Authorization")
                    == "Bearer dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                )
            if expected_client_creds:
                assert kwargs.get("client_id") == "foo"
                assert kwargs.get("client_secret") == "bar"
            assert session_configuration["spark.sql.ansi.enabled"] == "true"
            if expected_catalog is None:
                assert catalog is None
            else:
                assert catalog == expected_catalog
            if expected_invocation_env is not None:
                assert (
                    _user_agent_entry
                    == f"dbt-databricks/{__version__.version}; {expected_invocation_env}"
                )
            else:
                assert _user_agent_entry == f"dbt-databricks/{__version__.version}"
            if expected_http_headers is None:
                assert http_headers is None
            else:
                assert http_headers == expected_http_headers
            return Mock()

        return connect

    def test_databricks_sql_connector_connection(self):
        self._test_databricks_sql_connector_connection(self._connect_func())

    def _test_databricks_sql_connector_connection(self, connect):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch("dbt.adapters.databricks.handle.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            assert connection.state == "open"
            assert connection.handle
            assert (
                connection.credentials.http_path
                == "sql/protocolv1/o/1234567890123456/1234-567890-test123"
            )
            assert connection.credentials.token == "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            assert connection.credentials.schema == "analytics"
            assert len(connection.credentials.session_properties) == 1
            assert connection.credentials.session_properties["spark.sql.ansi.enabled"] == "true"

    def test_databricks_sql_connector_catalog_connection(self):
        self._test_databricks_sql_connector_catalog_connection(
            self._connect_func(expected_catalog="main")
        )

    def _test_databricks_sql_connector_catalog_connection(self, connect):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch("dbt.adapters.databricks.handle.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            assert connection.state == "open"
            assert connection.handle
            assert (
                connection.credentials.http_path
                == "sql/protocolv1/o/1234567890123456/1234-567890-test123"
            )
            assert connection.credentials.token == "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            assert connection.credentials.schema == "analytics"
            assert connection.credentials.database == "main"

    def test_databricks_sql_connector_http_header_connection(self):
        self._test_databricks_sql_connector_http_header_connection(
            {"aaa": "xxx"}, self._connect_func(expected_http_headers=[("aaa", "xxx")])
        )
        self._test_databricks_sql_connector_http_header_connection(
            {"aaa": "xxx", "bbb": "yyy"},
            self._connect_func(expected_http_headers=[("aaa", "xxx"), ("bbb", "yyy")]),
        )

    def _test_databricks_sql_connector_http_header_connection(self, http_headers, connect):
        config = self._get_config(connection_parameters={"http_headers": http_headers})
        adapter = DatabricksAdapter(config, get_context("spawn"))

        with patch("dbt.adapters.databricks.handle.dbsql.connect", new=connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            assert connection.state == "open"
            assert connection.handle
            assert (
                connection.credentials.http_path
                == "sql/protocolv1/o/1234567890123456/1234-567890-test123"
            )
            assert connection.credentials.token == "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            assert connection.credentials.schema == "analytics"

    @patch("dbt.adapters.databricks.api_client.DatabricksApiClient.create")
    def test_list_relations_without_caching__no_relations(self, _):
        with patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = []
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            assert adapter.list_relations("database", "schema") == []

    @patch("dbt.adapters.databricks.api_client.DatabricksApiClient.create")
    def test_list_relations_without_caching__some_relations(self, _):
        with patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = [
                DatabricksRelationInfo("name", "table", "hudi", "owner", "external")
            ]
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            relations = adapter.list_relations("database", "schema")
            assert len(relations) == 1
            relation = relations[0]
            assert relation.identifier == "name"
            assert relation.database == "database"
            assert relation.schema == "schema"
            assert relation.type == DatabricksRelationType.Table
            assert relation.databricks_table_type == DatabricksTableType.External
            assert relation.owner == "owner"
            assert relation.is_external_table
            assert relation.is_hudi

    @patch("dbt.adapters.databricks.api_client.DatabricksApiClient.create")
    def test_list_relations_without_caching__hive_relation(self, _):
        with patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = [DatabricksRelationInfo("name", "table", None, None, None)]
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            relations = adapter.list_relations("database", "schema")
            assert len(relations) == 1
            relation = relations[0]
            assert relation.identifier == "name"
            assert relation.database == "database"
            assert relation.schema == "schema"
            assert relation.type == DatabricksRelationType.Table
            assert not relation.has_information()

    @patch("dbt.adapters.databricks.api_client.DatabricksApiClient.create")
    def test_get_schema_for_catalog__no_columns(self, _):
        with patch.object(DatabricksAdapter, "_list_relations_with_information") as list_info:
            list_info.return_value = [(Mock(), "info")]
            with patch.object(DatabricksAdapter, "_get_columns_for_catalog") as get_columns:
                get_columns.return_value = []
                adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
                table = adapter._get_schema_for_catalog("database", "schema", "name")
                assert len(table.rows) == 0

    @patch("dbt.adapters.databricks.api_client.DatabricksApiClient.create")
    def test_get_schema_for_catalog__some_columns(self, _):
        with patch.object(DatabricksAdapter, "_list_relations_with_information") as list_info:
            list_info.return_value = [(Mock(), "info")]
            with patch.object(DatabricksAdapter, "_get_columns_for_catalog") as get_columns:
                get_columns.return_value = [
                    {"name": "col1", "type": "string", "comment": "comment"},
                    {"name": "col2", "type": "string", "comment": "comment"},
                ]
                adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
                table = adapter._get_schema_for_catalog("database", "schema", "name")
                assert len(table.rows) == 2
                assert table.column_names == ("name", "type", "comment")

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

    def expected_column(self, real_vals):
        default_col = {
            "table_database": None,
            "table_schema": None,
            "table_name": None,
            "table_type": "table",
            "table_owner": "root",
            "table_comment": None,
            "column": "col1",
            "column_index": 0,
            "dtype": None,
            "mask": None,
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "comment": None,
            "not_null": None,
            "databricks_tags": None,
        }

        default_col.update(real_vals)
        return default_col

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)", "comment"),
            ("col2", "string", "comment"),
            ("dt", "date", None),
            ("struct_col", "struct<struct_inner_col:string>", None),
            ("# Partition Information", "data_type", None),
            ("# col_name", "data_type", "comment"),
            ("dt", "date", None),
            (None, None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root", None),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815", None),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925", None),
            ("Type", "MANAGED", None),
            ("Provider", "delta", None),
            ("Location", "/mnt/vo", None),
            (
                "Serde Library",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                None,
            ),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat", None),
            (
                "OutputFormat",
                "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                None,
            ),
            ("Partition Provider", "Catalog", None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        config = self._get_config()
        metadata, rows = DatabricksAdapter(config, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )

        assert metadata == {
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
        }

        assert len(rows) == 4
        assert rows[0].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "comment": "comment",
            }
        )

        assert rows[1].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "comment": "comment",
            }
        )

        assert rows[2].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
            }
        )

        assert rows[3].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
            }
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
            ("col1", "decimal(22,0)", "comment"),
            ("# Detailed Table Information", None, None),
            ("Owner", 1234, None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        config = self._get_config()
        _, rows = DatabricksAdapter(config, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )

        assert rows[0].to_column_dict().get("table_owner") == "1234"

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = DatabricksRelation.get_relation_type.Table

        relation = DatabricksRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)", "comment"),
            ("# Partition Information", "data_type", None),
            (None, None, None),
            ("# Detailed Table Information", None, None),
            ("Database", None, None),
            ("Owner", "root", None),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815", None),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925", None),
            ("Comment", "Table model description", None),
            ("Statistics", "1109049927 bytes, 14093476 rows", None),
            ("Type", "MANAGED", None),
            ("Provider", "delta", None),
            ("Location", "/mnt/vo", None),
            (
                "Serde Library",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                None,
            ),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat", None),
            (
                "OutputFormat",
                "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                None,
            ),
            ("Partition Provider", "Catalog", None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        config = self._get_config()
        metadata, rows = DatabricksAdapter(config, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )

        assert metadata == {
            None: None,
            "# Detailed Table Information": None,
            "Database": None,
            "Owner": "root",
            "Created Time": "Wed Feb 04 18:15:00 UTC 1815",
            "Last Access": "Wed May 20 19:25:00 UTC 1925",
            "Comment": "Table model description",
            "Statistics": "1109049927 bytes, 14093476 rows",
            "Type": "MANAGED",
            "Provider": "delta",
            "Location": "/mnt/vo",
            "Serde Library": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "InputFormat": "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "Partition Provider": "Catalog",
        }

        assert len(rows) == 1
        assert rows[0].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "table_comment": "Table model description",
                "column": "col1",
                "column_index": 0,
                "comment": "comment",
                "dtype": "decimal(22,0)",
                "mask": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            }
        )

    def test_relation_with_database(self):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))
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

        config = self._get_config()
        columns = DatabricksAdapter(config, get_context("spawn")).parse_columns_from_information(
            relation, information
        )
        assert len(columns) == 4
        assert columns[0].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            }
        )

        assert columns[3].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            }
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

        config = self._get_config()
        columns = DatabricksAdapter(config, get_context("spawn")).parse_columns_from_information(
            relation, information
        )
        assert len(columns) == 4
        assert columns[1].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
            }
        )

        assert columns[3].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
            }
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

        config = self._get_config()
        columns = DatabricksAdapter(config, get_context("spawn")).parse_columns_from_information(
            relation, information
        )
        assert len(columns) == 4
        assert columns[2].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            }
        )

        assert columns[3].to_column_dict(omit_none=False) == self.expected_column(
            {
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            }
        )

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

    def test_generate_unique_temporary_table_suffix_adds_unique_identifier(self):
        suffix_initial = "dbt_tmp"

        result = DatabricksAdapter.generate_unique_temporary_table_suffix(suffix_initial)
        assert result != suffix_initial
        assert result.startswith(suffix_initial)
        assert result != DatabricksAdapter.generate_unique_temporary_table_suffix(suffix_initial)

    def test_generate_unique_temporary_table_suffix_generates_no_new_illegal_characters(self):
        suffix_initial = "dbt_tmp"

        result = DatabricksAdapter.generate_unique_temporary_table_suffix(suffix_initial)
        assert not re.match(r"[^A-Za-z0-9_]+", result.replace(suffix_initial, ""))


class TestCheckNotFound:
    def test_prefix(self):
        assert check_not_found_error("Runtime error \n Database 'dbt' not found")

    def test_no_prefix_or_suffix(self):
        assert check_not_found_error("Database not found")

    def test_quotes(self):
        assert check_not_found_error("Database '`dbt`' not found")

    def test_suffix(self):
        assert check_not_found_error("Database not found and \n foo")

    def test_error_condition(self):
        assert check_not_found_error("[SCHEMA_NOT_FOUND]")

    def test_unexpected_error(self):
        assert not check_not_found_error("[DATABASE_NOT_FOUND]")
        assert not check_not_found_error("Schema foo not found")
        assert not check_not_found_error("Database 'foo' not there")


class TestGetPersistDocColumns(DatabricksAdapterBase):
    @pytest.fixture
    def adapter(self, setUp) -> DatabricksAdapter:
        return DatabricksAdapter(self._get_config(), get_context("spawn"))

    def create_column(self, name, comment) -> DatabricksColumn:
        return DatabricksColumn(
            column=name,
            dtype="string",
            comment=comment,
        )

    def test_get_persist_doc_columns_empty(self, adapter):
        assert adapter.get_persist_doc_columns([], {}) == {}

    def test_get_persist_doc_columns_no_match(self, adapter):
        existing = [self.create_column("col1", "comment1")]
        column_dict = {"col2": {"name": "col2", "description": "comment2"}}
        assert adapter.get_persist_doc_columns(existing, column_dict) == {}

    def test_get_persist_doc_columns_full_match(self, adapter):
        existing = [self.create_column("col1", "comment1")]
        column_dict = {"col1": {"name": "col1", "description": "comment1"}}
        assert adapter.get_persist_doc_columns(existing, column_dict) == {}

    def test_get_persist_doc_columns_partial_match(self, adapter):
        existing = [self.create_column("col1", "comment1")]
        column_dict = {"col1": {"name": "col1", "description": "comment2"}}
        assert adapter.get_persist_doc_columns(existing, column_dict) == column_dict

    def test_get_persist_doc_columns_mixed(self, adapter):
        existing = [
            self.create_column("col1", "comment1"),
            self.create_column("col2", "comment2"),
        ]
        column_dict = {
            "col1": {"name": "col1", "description": "comment2"},
            "col2": {"name": "col2", "description": "comment2"},
        }
        expected = {
            "col1": {"name": "col1", "description": "comment2"},
        }
        assert adapter.get_persist_doc_columns(existing, column_dict) == expected


class TestGetColumnsByDbrVersion(DatabricksAdapterBase):
    @pytest.fixture
    def adapter(self, setUp) -> DatabricksAdapter:
        return DatabricksAdapter(self._get_config(), get_context("spawn"))

    @pytest.fixture
    def unity_relation(self):
        """Relation for Unity Catalog (not hive metastore)"""
        return DatabricksRelation.create(
            database="test_catalog",  # Unity catalog database
            schema="test_schema",
            identifier="test_table",
            type=DatabricksRelation.Table,
        )

    @patch(
        "dbt.adapters.databricks.behaviors.columns.GetColumnsByDescribe._get_columns_with_comments"
    )
    def test_get_columns_legacy_logic(self, mock_get_columns, adapter, unity_relation):
        # Return value less than 0 means version is older than 16.2
        with patch.object(adapter, "compare_dbr_version", return_value=-1):
            mock_get_columns.return_value = [
                {"col_name": "col1", "data_type": "string", "comment": "comment1"},
            ]

            result = adapter.get_columns_in_relation(unity_relation)
            mock_get_columns.assert_called_with(adapter, unity_relation, "get_columns_comments")

            assert len(result) == 1
            assert result[0].column == "col1"
            assert result[0].dtype == "string"

    @patch(
        "dbt.adapters.databricks.behaviors.columns.GetColumnsByDescribe._get_columns_with_comments"
    )
    def test_get_columns_new_logic(self, mock_get_columns, adapter, unity_relation):
        # Return value 0 means version is 16.2
        with patch.object(adapter, "compare_dbr_version", return_value=0):
            json_data = (
                '{"columns": [{"name": "col1", "type": {"name": "string"}, "comment": "comment1"}]}'
            )
            mock_get_columns.return_value = [{"json_metadata": json_data}]
            result = adapter.get_columns_in_relation(unity_relation)

            mock_get_columns.assert_called_with(
                adapter, unity_relation, "get_columns_comments_as_json"
            )

            assert len(result) == 1
            assert result[0].column == "col1"
            assert result[0].dtype == "string"
            assert result[0].comment == "comment1"

    @patch(
        "dbt.adapters.databricks.behaviors.columns.GetColumnsByDescribe._get_columns_with_comments"
    )
    def test_get_columns_streaming_table_legacy_logic(
        self, mock_get_columns, adapter, unity_relation
    ):
        streaming_relation = DatabricksRelation.create(
            database=unity_relation.database,
            schema=unity_relation.schema,
            identifier=unity_relation.identifier,
            type=DatabricksRelation.StreamingTable,
        )
        # Return value less than 0 means version is older than 17.1
        with patch.object(adapter, "compare_dbr_version", return_value=-1):
            mock_get_columns.return_value = [
                {"col_name": "stream_col", "data_type": "int", "comment": "streaming col"},
            ]
            result = adapter.get_columns_in_relation(streaming_relation)
            mock_get_columns.assert_called_with(adapter, streaming_relation, "get_columns_comments")
            assert len(result) == 1
            assert result[0].column == "stream_col"
            assert result[0].dtype == "int"
            assert result[0].comment == "streaming col"

    @patch(
        "dbt.adapters.databricks.behaviors.columns.GetColumnsByDescribe._get_columns_with_comments"
    )
    def test_get_columns_streaming_table_new_logic(self, mock_get_columns, adapter, unity_relation):
        streaming_relation = DatabricksRelation.create(
            database=unity_relation.database,
            schema=unity_relation.schema,
            identifier=unity_relation.identifier,
            type=DatabricksRelation.StreamingTable,
        )
        # Return value 0 means version is 17.1
        with patch.object(adapter, "compare_dbr_version", return_value=0):
            json_data = """
                {
                  "columns": [
                    {
                      "name": "stream_col",
                      "type": {"name": "int"},
                      "comment": "streaming col"
                    }
                  ]
                }
                """
            mock_get_columns.return_value = [{"json_metadata": json_data}]
            result = adapter.get_columns_in_relation(streaming_relation)
            mock_get_columns.assert_called_with(
                adapter, streaming_relation, "get_columns_comments_as_json"
            )
            assert len(result) == 1
            assert result[0].column == "stream_col"
            assert result[0].dtype == "int"
            assert result[0].comment == "streaming col"
