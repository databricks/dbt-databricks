import json
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
import itertools
import os
import re
import sys
import time
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

logger = AdapterLogger("Databricks")

CATALOG_KEY_IN_SESSION_PROPERTIES = "databricks.catalog"
DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")
DBT_DATABRICKS_INVOCATION_ENV = "DBT_DATABRICKS_INVOCATION_ENV"
DBT_DATABRICKS_INVOCATION_ENV_REGEX = re.compile("^[A-z0-9\\-]+$")
EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/(.*)")
DBT_DATABRICKS_HTTP_SESSION_HEADERS = "DBT_DATABRICKS_HTTP_SESSION_HEADERS"


@dataclass
class DatabricksCredentials(Credentials):
    database: Optional[str]  # type: ignore[assignment]
    host: Optional[str] = None
    http_path: Optional[str] = None
    token: Optional[str] = None
    session_properties: Optional[Dict[str, Any]] = None
    connection_parameters: Optional[Dict[str, Any]] = None

    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_all: bool = False

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
        self.connection_parameters = connection_parameters

    def validate_creds(self) -> None:
        for key in ["host", "http_path", "token"]:
            if not getattr(self, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required to connect to Databricks".format(key)
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


class DatabricksSQLConnectionWrapper:
    """Wrap a Databricks SQL connector in a way that no-ops transactions"""

    _conn: DatabricksSQLConnection
    _is_cluster: bool
    _cursors: List[DatabricksSQLCursor]

    def __init__(self, conn: DatabricksSQLConnection, *, is_cluster: bool):
        self._conn = conn
        self._is_cluster = is_cluster
        self._cursors = []

    def cursor(self) -> "DatabricksSQLCursorWrapper":
        cursor = self._conn.cursor()
        self._cursors.append(cursor)
        return DatabricksSQLCursorWrapper(cursor)

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

    def __init__(self, cursor: DatabricksSQLCursor):
        self._cursor = cursor

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
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]
        self._cursor.execute(sql, bindings)

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


class DatabricksConnectionManager(SparkConnectionManager):
    TYPE: str = "databricks"

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
                        status=str(self.get_response(cursor)), elapsed=round((time.time() - pre), 2)
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
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = self.get_response(cursor)
            if fetch:
                table = self.get_result_from_cursor(cursor)
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
                        status=str(self.get_response(cursor)), elapsed=round((time.time() - pre), 2)
                    )
                )

                return self.get_result_from_cursor(cursor)
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

        creds.validate_creds()

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
                    access_token=creds.token,
                    http_headers=http_headers if http_headers else None,
                    session_configuration=creds.session_properties,
                    catalog=creds.database,
                    # schema=creds.schema,  # TODO: Explicitly set once DBR 7.3LTS is EOL.
                    _user_agent_entry=user_agent_entry,
                    **connection_parameters,
                )
                return DatabricksSQLConnectionWrapper(conn, is_cluster=creds.cluster_id is not None)
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


def _log_dbsql_errors(exc: Exception) -> None:
    if isinstance(exc, Error):
        logger.debug(f"{type(exc)}: {exc}")
        for key, value in sorted(exc.context.items()):
            logger.debug(f"{key}: {value}")
