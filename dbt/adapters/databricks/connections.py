import json
import uuid
import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
import itertools
import os
import re
import sys
import threading
import time
import requests
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
    Union,
)

from agate import Table

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.spark.connections import SparkConnectionManager
from dbt.clients import agate_helper
from dbt.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    DEFAULT_QUERY_COMMENT,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.utils import DECIMALS, cast_to_str

from databricks import sql as dbsql
from databricks.sql.client import (
    Connection as DatabricksSQLConnection,
    Cursor as DatabricksSQLCursor,
)
from databricks.sql.exc import Error

from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.utils import redact_credentials

from databricks.sdk.core import CredentialsProvider
from databricks.sdk.oauth import OAuthClient, SessionCredentials
from dbt.adapters.databricks.auth import token_auth, m2m_auth

import keyring

logger = AdapterLogger("Databricks")


class DbtCoreHandler(logging.Handler):
    def __init__(self, level: Union[str, int], dbt_logger: AdapterLogger):
        super().__init__(level=level)
        self.logger = dbt_logger

    def emit(self, record: logging.LogRecord) -> None:
        # record.levelname will be debug, info, warning, error, or critical
        # these map 1-to-1 with methods of the AdapterLogger
        log_func = getattr(self.logger, record.levelname.lower())
        log_func(record.msg)


dbt_adapter_logger = AdapterLogger("databricks-sql-connector")

pysql_logger = logging.getLogger("databricks.sql")
pysql_logger_level = os.environ.get("DBT_DATABRICKS_CONNECTOR_LOG_LEVEL", "INFO").upper()
pysql_logger.setLevel(pysql_logger_level)

pysql_handler = DbtCoreHandler(dbt_logger=dbt_adapter_logger, level=pysql_logger_level)
pysql_logger.addHandler(pysql_handler)


CATALOG_KEY_IN_SESSION_PROPERTIES = "databricks.catalog"
DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")
DBT_DATABRICKS_INVOCATION_ENV = "DBT_DATABRICKS_INVOCATION_ENV"
DBT_DATABRICKS_INVOCATION_ENV_REGEX = re.compile("^[A-z0-9\\-]+$")
EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/(.*)")
DBT_DATABRICKS_HTTP_SESSION_HEADERS = "DBT_DATABRICKS_HTTP_SESSION_HEADERS"

REDIRECT_URL = "http://localhost:8020"
CLIENT_ID = "dbt-databricks"
SCOPES = ["all-apis", "offline_access"]


@dataclass
class DatabricksCredentials(Credentials):
    database: Optional[str]  # type: ignore[assignment]
    host: Optional[str] = None
    http_path: Optional[str] = None
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    session_properties: Optional[Dict[str, Any]] = None
    connection_parameters: Optional[Dict[str, Any]] = None
    auth_type: Optional[str] = None

    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_all: bool = False

    _credentials_provider: Optional[Dict[str, Any]] = None
    _lock = threading.Lock()  # to avoid concurrent auth

    _ALIASES = {
        "catalog": "database",
        "target_catalog": "target_database",
    }

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self) -> None:
        if "." in self.schema:
            raise dbt.exceptions.DbtValidationError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use `catalog` instead.\n"
            )

        session_properties = self.session_properties or {}
        if CATALOG_KEY_IN_SESSION_PROPERTIES in session_properties:
            if self.database is None:
                self.database = session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
                del session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
            else:
                raise dbt.exceptions.DbtValidationError(
                    f"Got duplicate keys: (`{CATALOG_KEY_IN_SESSION_PROPERTIES}` "
                    'in session_properties) all map to "database"'
                )
        self.session_properties = session_properties

        if self.database is not None:
            database = self.database.strip()
            if not database:
                raise dbt.exceptions.DbtValidationError(
                    f"Invalid catalog name : `{self.database}`."
                )
            self.database = database

        connection_parameters = self.connection_parameters or {}
        for key in (
            "server_hostname",
            "http_path",
            "access_token",
            "client_id",
            "client_secret",
            "session_configuration",
            "catalog",
            "schema",
            "_user_agent_entry",
        ):
            if key in connection_parameters:
                raise dbt.exceptions.DbtValidationError(
                    f"The connection parameter `{key}` is reserved."
                )
        if "http_headers" in connection_parameters:
            http_headers = connection_parameters["http_headers"]
            if not isinstance(http_headers, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in http_headers.items()
            ):
                raise dbt.exceptions.DbtValidationError(
                    "The connection parameter `http_headers` should be dict of strings: "
                    f"{http_headers}."
                )
        if "_socket_timeout" not in connection_parameters:
            connection_parameters["_socket_timeout"] = 180
        self.connection_parameters = connection_parameters

    def validate_creds(self) -> None:
        for key in ["host", "http_path"]:
            if not getattr(self, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required to connect to Databricks".format(key)
                )
        if not self.token and self.auth_type != "oauth":
            raise dbt.exceptions.DbtProfileError(
                ("The config `auth_type: oauth` is required when not using access token")
            )

        if not self.client_id and self.client_secret:
            raise dbt.exceptions.DbtProfileError(
                (
                    "The config 'client_id' is required to connect "
                    "to Databricks when 'client_secret' is present"
                )
            )

    @classmethod
    def get_invocation_env(cls) -> Optional[str]:
        invocation_env = os.environ.get(DBT_DATABRICKS_INVOCATION_ENV)
        if invocation_env:
            # Thrift doesn't allow nested () so we need to ensure
            # that the passed user agent is valid.
            if not DBT_DATABRICKS_INVOCATION_ENV_REGEX.search(invocation_env):
                raise dbt.exceptions.DbtValidationError(
                    f"Invalid invocation environment: {invocation_env}"
                )
        return invocation_env

    @classmethod
    def get_all_http_headers(cls, user_http_session_headers: Dict[str, str]) -> Dict[str, str]:
        http_session_headers_str: Optional[str] = os.environ.get(
            DBT_DATABRICKS_HTTP_SESSION_HEADERS
        )

        http_session_headers_dict: Dict[str, str] = (
            {
                k: v if isinstance(v, str) else json.dumps(v)
                for k, v in json.loads(http_session_headers_str).items()
            }
            if http_session_headers_str is not None
            else {}
        )

        intersect_http_header_keys = (
            user_http_session_headers.keys() & http_session_headers_dict.keys()
        )

        if len(intersect_http_header_keys) > 0:
            raise dbt.exceptions.DbtValidationError(
                f"Intersection with reserved http_headers in keys: {intersect_http_header_keys}"
            )

        http_session_headers_dict.update(user_http_session_headers)

        return http_session_headers_dict

    @property
    def type(self) -> str:
        return "databricks"

    @property
    def unique_field(self) -> str:
        return cast(str, self.host)

    def connection_info(self, *, with_aliases: bool = False) -> Iterable[Tuple[str, Any]]:
        as_dict = self.to_dict(omit_none=False)
        connection_keys = set(self._connection_keys(with_aliases=with_aliases))
        aliases: List[str] = []
        if with_aliases:
            aliases = [k for k, v in self._ALIASES.items() if v in connection_keys]
        for key in itertools.chain(self._connection_keys(with_aliases=with_aliases), aliases):
            if key in as_dict:
                yield key, as_dict[key]

    def _connection_keys(self, *, with_aliases: bool = False) -> Tuple[str, ...]:
        # Assuming `DatabricksCredentials.connection_info(self, *, with_aliases: bool = False)`
        # is called from only:
        #
        # - `Profile` with `with_aliases=True`
        # - `DebugTask` without `with_aliases` (`False` by default)
        #
        # Thus, if `with_aliases` is `True`, `DatabricksCredentials._connection_keys` should return
        # the internal key names; otherwise it can use aliases to show in `dbt debug`.
        connection_keys = ["host", "http_path", "schema"]
        if with_aliases:
            connection_keys.insert(2, "database")
        elif self.database:
            connection_keys.insert(2, "catalog")
        if self.session_properties:
            connection_keys.append("session_properties")
        return tuple(connection_keys)

    @classmethod
    def extract_cluster_id(cls, http_path: str) -> Optional[str]:
        m = EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX.match(http_path)
        if m:
            return m.group(1).strip()
        else:
            return None

    @property
    def cluster_id(self) -> Optional[str]:
        return self.extract_cluster_id(self.http_path)  # type: ignore[arg-type]

    def authenticate(self, in_provider: CredentialsProvider) -> CredentialsProvider:
        self.validate_creds()
        host: str = self.host or ""
        if self._credentials_provider:
            return self._provider_from_dict()
        if in_provider:
            self._credentials_provider = in_provider.as_dict()
            return in_provider

        # dbt will spin up multiple threads. This has to be sync. So lock here
        self._lock.acquire()
        try:
            if self.token:
                provider = token_auth(self.token)
                self._credentials_provider = provider.as_dict()
                return provider

            if self.client_id and self.client_secret:
                provider = m2m_auth(
                    host=host,
                    client_id=self.client_id or "",
                    client_secret=self.client_secret or "",
                )
                self._credentials_provider = provider.as_dict()
                return provider

            oauth_client = OAuthClient(
                host=host,
                client_id=self.client_id if self.client_id else CLIENT_ID,
                client_secret=None,
                redirect_url=REDIRECT_URL,
                scopes=SCOPES,
            )
            # optional branch. Try and keep going if it does not work
            try:
                # try to get cached credentials
                credsdict = keyring.get_password("dbt-databricks", host)

                if credsdict:
                    provider = SessionCredentials.from_dict(oauth_client, json.loads(credsdict))
                    # if refresh token is expired, this will throw
                    try:
                        if provider.token().valid:
                            return provider
                    except Exception as e:
                        logger.debug(e)
                        # whatever it is, get rid of the cache
                        keyring.delete_password("dbt-databricks", host)

            # error with keyring. Maybe machine has no password persistency
            except Exception as e:
                logger.debug(e)
                logger.info("could not retrieved saved token")

            # no token, go fetch one
            consent = oauth_client.initiate_consent()

            provider = consent.launch_external_browser()
            # save for later
            self._credentials_provider = provider.as_dict()
            try:
                keyring.set_password("dbt-databricks", host, json.dumps(self._credentials_provider))
            # error with keyring. Maybe machine has no password persistency
            except Exception as e:
                logger.debug(e)
                logger.info("could not save token")

            return provider

        finally:
            self._lock.release()

    def _provider_from_dict(self) -> CredentialsProvider:
        if self.token:
            return token_auth.from_dict(self._credentials_provider)

        if self.client_id and self.client_secret:
            return m2m_auth.from_dict(
                host=self.host or "",
                client_id=self.client_id or "",
                client_secret=self.client_secret or "",
                raw=self._credentials_provider or {"token": {}},
            )

        oauth_client = OAuthClient(
            host=self.host,
            client_id=CLIENT_ID,
            client_secret=None,
            redirect_url=REDIRECT_URL,
            scopes=SCOPES,
        )

        return SessionCredentials.from_dict(client=oauth_client, raw=self._credentials_provider)


class DatabricksSQLConnectionWrapper:
    """Wrap a Databricks SQL connector in a way that no-ops transactions"""

    _conn: DatabricksSQLConnection
    _is_cluster: bool
    _cursors: List[DatabricksSQLCursor]
    _creds: DatabricksCredentials
    _user_agent: str

    def __init__(
        self,
        conn: DatabricksSQLConnection,
        *,
        is_cluster: bool,
        creds: DatabricksCredentials,
        user_agent: str,
    ):
        self._conn = conn
        self._is_cluster = is_cluster
        self._cursors = []
        self._creds = creds
        self._user_agent = user_agent

    def cursor(self) -> "DatabricksSQLCursorWrapper":
        cursor = self._conn.cursor()
        self._cursors.append(cursor)
        return DatabricksSQLCursorWrapper(
            cursor,
            creds=self._creds,
            user_agent=self._user_agent,
        )

    def cancel(self) -> None:
        cursors: List[DatabricksSQLCursor] = self._cursors

        for cursor in cursors:
            try:
                cursor.cancel()
            except Error as exc:
                logger.debug("Exception while cancelling query: {}".format(exc))
                _log_dbsql_errors(exc)

    def close(self) -> None:
        try:
            self._conn.close()
        except Error as exc:
            logger.debug("Exception while closing connection: {}".format(exc))
            _log_dbsql_errors(exc)

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    _dbr_version: Tuple[int, int]

    @property
    def dbr_version(self) -> Tuple[int, int]:
        if not hasattr(self, "_dbr_version"):
            if self._is_cluster:
                with self._conn.cursor() as cursor:
                    cursor.execute("SET spark.databricks.clusterUsageTags.sparkVersion")
                    dbr_version: str = cursor.fetchone()[1]

                m = DBR_VERSION_REGEX.search(dbr_version)
                assert m, f"Unknown DBR version: {dbr_version}"
                major = int(m.group(1))
                try:
                    minor = int(m.group(2))
                except ValueError:
                    minor = sys.maxsize
                self._dbr_version = (major, minor)
            else:
                # Assuming SQL Warehouse uses the latest version.
                self._dbr_version = (sys.maxsize, sys.maxsize)

        return self._dbr_version


class DatabricksSQLCursorWrapper:
    """Wrap a Databricks SQL cursor in a way that no-ops transactions"""

    _cursor: DatabricksSQLCursor
    _user_agent: str
    _creds: DatabricksCredentials

    def __init__(self, cursor: DatabricksSQLCursor, creds: DatabricksCredentials, user_agent: str):
        self._cursor = cursor
        self._creds = creds
        self._user_agent = user_agent

    def cancel(self) -> None:
        try:
            self._cursor.cancel()
        except Error as exc:
            logger.debug("Exception while cancelling query: {}".format(exc))
            _log_dbsql_errors(exc)

    def close(self) -> None:
        try:
            self._cursor.close()
        except Error as exc:
            logger.debug("Exception while closing cursor: {}".format(exc))
            _log_dbsql_errors(exc)

    def fetchall(self) -> Sequence[Tuple]:
        return self._cursor.fetchall()

    def fetchone(self) -> Optional[Tuple]:
        return self._cursor.fetchone()

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        # print(f"execute: {sql}")
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]
        self._cursor.execute(sql, bindings)

        # if the command was to refresh a materialized view we need to poll
        # the pipeline until the refresh is finished.
        self.pollRefreshPipeline(sql)

    def pollRefreshPipeline(
        self,
        sql: str,
    ) -> None:
        should_poll, model_name = _should_poll_refresh(sql)
        if not should_poll:
            return

        # interval in seconds
        polling_interval = 10

        # timeout in seconds
        timeout = 60 * 60

        stopped_states = ("COMPLETED", "FAILED", "CANCELED")
        host: str = self._creds.host or ""
        headers = self._cursor.connection.thrift_backend._auth_provider._header_factory()
        headers["User-Agent"] = self._user_agent

        pipeline_id = _get_table_view_pipeline_id(host, headers, model_name)
        pipeline = _get_pipeline_state(host, headers, pipeline_id)
        # get the most recently created update for the pipeline
        latest_update = _find_update(pipeline)
        if not latest_update:
            raise dbt.exceptions.DbtRuntimeError(f"No update created for pipeline: {pipeline_id}")

        state = latest_update.get("state")
        # we use update_id to retrieve the update in the polling loop
        update_id = latest_update.get("update_id", "")
        prev_state = state

        logger.info(
            f"refreshing {model_name}, pipeline: {pipeline_id}, update: {update_id} {state}"
        )

        start = time.time()
        exceeded_timeout = False
        while state not in stopped_states:
            if time.time() - start > timeout:
                exceeded_timeout = True
                break

            # should we do exponential backoff?
            time.sleep(polling_interval)

            pipeline = _get_pipeline_state(host, headers, pipeline_id)
            # get the update we are currently polling
            update = _find_update(pipeline, update_id)
            if not update:
                raise dbt.exceptions.DbtRuntimeError(
                    f"Error getting pipeline update info: {pipeline_id}, update: {update_id}"
                )

            state = update.get("state")
            if state != prev_state:
                logger.info(
                    f"refreshing {model_name}, pipeline: {pipeline_id}, update: {update_id} {state}"
                )
                prev_state = state

            if state == "FAILED":
                logger.error(f"pipeline {pipeline_id} update {update_id} failed")
                msg = _get_update_error_msg(host, headers, pipeline_id, update_id)
                if msg:
                    logger.error(msg)

                # another update may have been created due to retry_on_fail settings
                # get the latest update and see if it is a new one
                latest_update = _find_update(pipeline)
                if not latest_update:
                    raise dbt.exceptions.DbtRuntimeError(
                        f"No update created for pipeline: {pipeline_id}"
                    )

                latest_update_id = latest_update.get("update_id", "")
                if latest_update_id != update_id:
                    update_id = latest_update_id
                    state = None

        if exceeded_timeout:
            raise dbt.exceptions.DbtRuntimeError("timed out waiting for materialized view refresh")

        if state == "FAILED":
            msg = _get_update_error_msg(host, headers, pipeline_id, update_id)
            raise dbt.exceptions.DbtRuntimeError(f"error refreshing model {model_name} {msg}")

        if state == "CANCELED":
            raise dbt.exceptions.DbtRuntimeError(f"refreshing model {model_name} cancelled")

        return

    @classmethod
    def findUpdate(cls, updates: List, id: str) -> Optional[Dict]:
        matches = [x for x in updates if x.get("update_id") == id]
        if matches:
            return matches[0]

        return None

    @property
    def hex_query_id(self) -> str:
        """Return the hex GUID for this query

        This UUID can be tied back to the Databricks query history API
        """

        _as_hex = uuid.UUID(bytes=self._cursor.active_result_set.command_id.operationId.guid)

        return str(_as_hex)

    @classmethod
    def _fix_binding(cls, value: Any) -> Any:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, DECIMALS):
            return float(value)
        else:
            return value

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[
            str,
            str,
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[bool],
        ]
    ]:
        return self._cursor.description

    def schemas(self, catalog_name: str, schema_name: Optional[str] = None) -> None:
        self._cursor.schemas(catalog_name=catalog_name, schema_name=schema_name)

    def tables(self, catalog_name: str, schema_name: str, table_name: Optional[str] = None) -> None:
        self._cursor.tables(
            catalog_name=catalog_name, schema_name=schema_name, table_name=table_name
        )

    def __del__(self) -> None:
        if self._cursor.open:
            # This should not happen. The cursor should explicitly be closed.
            self._cursor.close()
            with warnings.catch_warnings():
                warnings.simplefilter("always")
                warnings.warn("The cursor was closed by destructor.")


DATABRICKS_QUERY_COMMENT = f"""
{{%- set comment_dict = {{}} -%}}
{{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    dbt_databricks_version='{__version__}',
    databricks_sql_connector_version='{dbsql.__version__}',
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}}
{{%- if node is not none -%}}
  {{%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}}
{{% else %}}
  {{# in the node context, the connection name is the node_id #}}
  {{%- do comment_dict.update(connection_name=connection_name) -%}}
{{%- endif -%}}
{{{{ return(tojson(comment_dict)) }}}}
"""


class DatabricksMacroQueryStringSetter(MacroQueryStringSetter):
    def _get_comment_macro(self) -> Optional[str]:
        if self.config.query_comment.comment == DEFAULT_QUERY_COMMENT:
            return DATABRICKS_QUERY_COMMENT
        else:
            return self.config.query_comment.comment


@dataclass
class DatabricksAdapterResponse(AdapterResponse):
    query_id: str = ""


class DatabricksConnectionManager(SparkConnectionManager):
    TYPE: str = "databricks"
    credentials_provider: CredentialsProvider = None

    def compare_dbr_version(self, major: int, minor: int) -> int:
        version = (major, minor)

        connection: DatabricksSQLConnectionWrapper = self.get_thread_connection().handle
        dbr_version = connection.dbr_version
        return (dbr_version > version) - (dbr_version < version)

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = DatabricksMacroQueryStringSetter(self.profile, manifest)

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        log_sql = redact_credentials(sql)

        try:
            yield

        except Error as exc:
            logger.debug(f"Error while running:\n{log_sql}")
            _log_dbsql_errors(exc)
            raise dbt.exceptions.DbtRuntimeError(str(exc)) from exc

        except Exception as exc:
            logger.debug(f"Error while running:\n{log_sql}")
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.DbtRuntimeError(msg) from exc
            else:
                raise dbt.exceptions.DbtRuntimeError(str(exc)) from exc

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        *,
        close_cursor: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=cast_to_str(connection.name)))

        with self.exception_handler(sql):
            cursor: Optional[DatabricksSQLCursorWrapper] = None
            try:
                log_sql = redact_credentials(sql)
                if abridge_sql_log:
                    log_sql = "{}...".format(log_sql[:512])

                fire_event(SQLQuery(conn_name=cast_to_str(connection.name), sql=log_sql))
                pre = time.time()

                cursor = cast(DatabricksSQLConnectionWrapper, connection.handle).cursor()
                cursor.execute(sql, bindings)

                fire_event(
                    SQLQueryStatus(
                        status=str(self.get_response(cursor)),
                        elapsed=round((time.time() - pre), 2),
                    )
                )

                return connection, cursor
            except Error:
                if cursor is not None:
                    cursor.close()
                    cursor = None
                raise
            finally:
                if close_cursor and cursor is not None:
                    cursor.close()

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[DatabricksAdapterResponse, Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = self.get_response(cursor)
            if fetch:
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = agate_helper.empty_table()
            return response, table
        finally:
            cursor.close()

    def _execute_cursor(
        self, log_sql: str, f: Callable[[DatabricksSQLCursorWrapper], None]
    ) -> Table:
        connection = self.get_thread_connection()

        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=cast_to_str(connection.name)))

        with self.exception_handler(log_sql):
            cursor: Optional[DatabricksSQLCursorWrapper] = None
            try:
                fire_event(SQLQuery(conn_name=cast_to_str(connection.name), sql=log_sql))
                pre = time.time()

                handle: DatabricksSQLConnectionWrapper = connection.handle
                cursor = handle.cursor()
                f(cursor)

                fire_event(
                    SQLQueryStatus(
                        status=str(self.get_response(cursor)),
                        elapsed=round((time.time() - pre), 2),
                    )
                )

                return self.get_result_from_cursor(cursor, None)
            finally:
                if cursor is not None:
                    cursor.close()

    def list_schemas(self, database: str, schema: Optional[str] = None) -> Table:
        return self._execute_cursor(
            f"GetSchemas(database={database}, schema={schema})",
            lambda cursor: cursor.schemas(catalog_name=database, schema_name=schema),
        )

    def list_tables(self, database: str, schema: str, identifier: Optional[str] = None) -> Table:
        return self._execute_cursor(
            f"GetTables(database={database}, schema={schema}, identifier={identifier})",
            lambda cursor: cursor.tables(
                catalog_name=database, schema_name=schema, table_name=identifier
            ),
        )

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds: DatabricksCredentials = connection.credentials
        timeout = creds.connect_timeout

        # gotta keep this so we don't prompt users many times
        cls.credentials_provider = creds.authenticate(cls.credentials_provider)

        user_agent_entry = f"dbt-databricks/{__version__}"

        invocation_env = creds.get_invocation_env()
        if invocation_env:
            user_agent_entry = f"{user_agent_entry}; {invocation_env}"

        connection_parameters = creds.connection_parameters.copy()  # type: ignore[union-attr]

        http_headers: List[Tuple[str, str]] = list(
            creds.get_all_http_headers(connection_parameters.pop("http_headers", {})).items()
        )

        def connect() -> DatabricksSQLConnectionWrapper:
            try:
                # TODO: what is the error when a user specifies a catalog they don't have access to
                conn: DatabricksSQLConnection = dbsql.connect(
                    server_hostname=creds.host,
                    http_path=creds.http_path,
                    credentials_provider=cls.credentials_provider,
                    http_headers=http_headers if http_headers else None,
                    session_configuration=creds.session_properties,
                    catalog=creds.database,
                    # schema=creds.schema,  # TODO: Explicitly set once DBR 7.3LTS is EOL.
                    _user_agent_entry=user_agent_entry,
                    **connection_parameters,
                )
                return DatabricksSQLConnectionWrapper(
                    conn,
                    is_cluster=creds.cluster_id is not None,
                    creds=creds,
                    user_agent=user_agent_entry,
                )
            except Error as exc:
                _log_dbsql_errors(exc)
                raise

        def exponential_backoff(attempt: int) -> int:
            return attempt * attempt

        retryable_exceptions = []
        # this option is for backwards compatibility
        if creds.retry_all:
            retryable_exceptions = [Error]

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retryable_exceptions=retryable_exceptions,
            retry_limit=creds.connect_retries,
            retry_timeout=(timeout if timeout is not None else exponential_backoff),
        )

    @classmethod
    def get_response(cls, cursor: DatabricksSQLCursorWrapper) -> DatabricksAdapterResponse:
        _query_id = getattr(cursor, "hex_query_id", None)
        if cursor is None:
            logger.debug("No cursor was provided. Query ID not available.")
            query_id = "N/A"
        else:
            query_id = _query_id
        message = "OK"
        return DatabricksAdapterResponse(_message=message, query_id=query_id)  # type: ignore


def _log_dbsql_errors(exc: Exception) -> None:
    if isinstance(exc, Error):
        logger.debug(f"{type(exc)}: {exc}")
        for key, value in sorted(exc.context.items()):
            logger.debug(f"{key}: {value}")


def _should_poll_refresh(sql: str) -> Tuple[bool, str]:
    # if the command was to refresh a materialized view we need to poll
    # the pipeline until the refresh is finished.
    name = ""
    refresh_search = re.search(r"refresh\s+materialized\s+view\s+([`\w.]+)", sql)
    if not refresh_search:
        refresh_search = re.search(r"create\s+or\s+refresh\s+streaming\s+table\s+([`\w.]+)", sql)

    if refresh_search:
        name = refresh_search.group(1).replace("`", "")

    return refresh_search is not None, name


def _get_table_view_pipeline_id(host: str, headers: dict, name: str) -> str:
    table_url = f"https://{host}/api/2.1/unity-catalog/tables/{name}"
    resp1 = requests.get(table_url, headers=headers)
    if resp1.status_code != 200:
        raise dbt.exceptions.DbtRuntimeError(
            f"Error getting info for materialized view/streaming table: {name}"
        )

    pipeline_id = resp1.json().get("pipeline_id", "")
    if not pipeline_id:
        raise dbt.exceptions.DbtRuntimeError(
            f"Materialized view/streaming table {name} does not have a pipeline id"
        )

    return pipeline_id


def _get_pipeline_state(host: str, headers: dict, pipeline_id: str) -> dict:
    pipeline_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}"

    response = requests.get(pipeline_url, headers=headers)
    if response.status_code != 200:
        raise dbt.exceptions.DbtRuntimeError(f"Error getting pipeline info: {pipeline_id}")

    return response.json()


def _find_update(pipeline: dict, id: str = "") -> Optional[Dict]:
    updates = pipeline.get("latest_updates", [])
    if not updates:
        raise dbt.exceptions.DbtRuntimeError(
            f"No updates for pipeline: {pipeline.get('pipeline_id', '')}"
        )

    if not id:
        return updates[0]

    matches = [x for x in updates if x.get("update_id") == id]
    if matches:
        return matches[0]

    return None


def _get_update_error_msg(host: str, headers: dict, pipeline_id: str, update_id: str) -> str:
    events_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}/events"
    response = requests.get(events_url, headers=headers)
    if response.status_code != 200:
        raise dbt.exceptions.DbtRuntimeError(f"Error getting pipeline event info: {pipeline_id}")

    events = response.json().get("events", [])
    update_events = [
        e
        for e in events
        if e.get("event_type", "") == "update_progress"
        and e.get("origin", {}).get("update_id") == update_id
    ]

    error_events = [
        e
        for e in update_events
        if e.get("details", {}).get("update_progress", {}).get("state", "") == "FAILED"
    ]

    msg = ""
    if error_events:
        msg = error_events[0].get("message", "")

    return msg
