from multiprocessing import get_context
from typing import Any
from typing import Dict
from typing import Optional

import dbt.flags as flags
import mock
import pytest
from agate import Row
from dbt.adapters.databricks import __version__
from dbt.adapters.databricks import DatabricksAdapter
from dbt.adapters.databricks import DatabricksRelation
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.credentials import CATALOG_KEY_IN_SESSION_PROPERTIES
from dbt.adapters.databricks.credentials import DBT_DATABRICKS_HTTP_SESSION_HEADERS
from dbt.adapters.databricks.credentials import DBT_DATABRICKS_INVOCATION_ENV
from dbt.adapters.databricks.utils import check_not_found_error
from dbt.adapters.databricks.impl import get_identifier_list_string
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.config import RuntimeConfig
from dbt_common.exceptions import DbtConfigError
from dbt_common.exceptions import DbtValidationError
from mock import Mock
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
        session_properties: Optional[Dict[str, str]] = {"spark.sql.ansi.enabled": "true"},
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
            "Got duplicate keys: (`databricks.catalog` in session_properties)"
            ' all map to "database"'
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
            with mock.patch.dict("os.environ", **{DBT_DATABRICKS_INVOCATION_ENV: "(Some-thing)"}):
                connection = adapter.acquire_connection("dummy")
                connection.handle  # trigger lazy-load

        assert "Invalid invocation environment" in str(excinfo.value)

    def test_custom_user_agent(self):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))

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

    @pytest.mark.skip("not ready")
    def test_oauth_settings(self):
        config = self._get_config(token=None)

        adapter = DatabricksAdapter(config, get_context("spawn"))

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
            new=self._connect_func(expected_no_token=True),
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

    @pytest.mark.skip("not ready")
    def test_client_creds_settings(self):
        config = self._get_config(client_id="foo", client_secret="bar")

        adapter = DatabricksAdapter(config, get_context("spawn"))

        with mock.patch(
            "dbt.adapters.databricks.connections.dbsql.connect",
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
                assert credentials_provider._token == "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

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

        return connect

    def test_databricks_sql_connector_connection(self):
        self._test_databricks_sql_connector_connection(self._connect_func())

    def _test_databricks_sql_connector_connection(self, connect):
        config = self._get_config()
        adapter = DatabricksAdapter(config, get_context("spawn"))

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
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

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
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

        with mock.patch("dbt.adapters.databricks.connections.dbsql.connect", new=connect):
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

    def test_list_relations_without_caching__no_relations(self):
        with mock.patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = []
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            assert adapter.list_relations("database", "schema") == []

    def test_list_relations_without_caching__some_relations(self):
        with mock.patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = [("name", "table", "hudi", "owner")]
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            relations = adapter.list_relations("database", "schema")
            assert len(relations) == 1
            relation = relations[0]
            assert relation.identifier == "name"
            assert relation.database == "database"
            assert relation.schema == "schema"
            assert relation.type == DatabricksRelationType.Table
            assert relation.owner == "owner"
            assert relation.is_hudi

    def test_list_relations_without_caching__hive_relation(self):
        with mock.patch.object(DatabricksAdapter, "get_relations_without_caching") as mocked:
            mocked.return_value = [("name", "table", None, None)]
            adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
            relations = adapter.list_relations("database", "schema")
            assert len(relations) == 1
            relation = relations[0]
            assert relation.identifier == "name"
            assert relation.database == "database"
            assert relation.schema == "schema"
            assert relation.type == DatabricksRelationType.Table
            assert not relation.has_information()

    def test_get_schema_for_catalog__no_columns(self):
        with mock.patch.object(DatabricksAdapter, "_list_relations_with_information") as list_info:
            list_info.return_value = [(Mock(), "info")]
            with mock.patch.object(DatabricksAdapter, "_get_columns_for_catalog") as get_columns:
                get_columns.return_value = []
                adapter = DatabricksAdapter(Mock(flags={}), get_context("spawn"))
                table = adapter._get_schema_for_catalog("database", "schema", "name")
                assert len(table.rows) == 0

    def test_get_schema_for_catalog__some_columns(self):
        with mock.patch.object(DatabricksAdapter, "_list_relations_with_information") as list_info:
            list_info.return_value = [(Mock(), "info")]
            with mock.patch.object(DatabricksAdapter, "_get_columns_for_catalog") as get_columns:
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
        assert rows[0].to_column_dict(omit_none=False) == {
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
            "comment": "comment",
        }

        assert rows[1].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "col2",
            "column_index": 1,
            "dtype": "string",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "comment": "comment",
        }

        assert rows[2].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "dt",
            "column_index": 2,
            "dtype": "date",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "comment": None,
        }

        assert rows[3].to_column_dict(omit_none=False) == {
            "table_database": None,
            "table_schema": relation.schema,
            "table_name": relation.name,
            "table_type": rel_type,
            "table_owner": "root",
            "table_comment": None,
            "column": "struct_col",
            "column_index": 3,
            "dtype": "struct<struct_inner_col:string>",
            "numeric_scale": None,
            "numeric_precision": None,
            "char_size": None,
            "comment": None,
        }

    def test_non_empty_acl_empty_config(self, _):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({}) == expected_access_control

    def test_non_empty_acl_non_empty_config(self, _):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({"a": "b"}) == {
            "a": "b",
            "access_control_list": expected_access_control["access_control_list"],
        }