import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing.context import SpawnContext
from typing import TYPE_CHECKING, Any, Optional, cast

from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtInternalError, DbtRuntimeError
from dbt_common.utils import cast_to_str

from databricks.sql import __version__ as dbsql_version
from databricks.sql.exc import Error
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.contracts.connection import (
    DEFAULT_QUERY_COMMENT,
    AdapterRequiredConfig,
    AdapterResponse,
    Connection,
    ConnectionState,
    Identifier,
    LazyHandle,
)
from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)
from dbt.adapters.databricks.events.connection_events import (
    ConnectionCreate,
    ConnectionCreateError,
    ConnectionIdleClose,
    ConnectionReset,
    ConnectionReuse,
)
from dbt.adapters.databricks.events.other_events import QueryError
from dbt.adapters.databricks.handle import CursorWrapper, DatabricksHandle, SqlUtils
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker
from dbt.adapters.databricks.utils import redact_credentials
from dbt.adapters.events.types import (
    ConnectionClosedInCleanup,
    ConnectionLeftOpenInCleanup,
    ConnectionReused,
    ConnectionUsed,
    NewConnection,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.spark.connections import SparkConnectionManager

if TYPE_CHECKING:
    from agate import Table


mv_refresh_regex = re.compile(r"refresh\s+materialized\s+view\s+([`\w.]+)", re.IGNORECASE)
st_refresh_regex = re.compile(
    r"create\s+or\s+refresh\s+streaming\s+table\s+([`\w.]+)", re.IGNORECASE
)


# Number of idle seconds before a connection is automatically closed. Only applicable if
# USE_LONG_SESSIONS is true.
# Updated when idle times of 180s were causing errors
DEFAULT_MAX_IDLE_TIME = 60


DATABRICKS_QUERY_COMMENT = f"""
{{%- set comment_dict = {{}} -%}}
{{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    dbt_databricks_version='{__version__}',
    databricks_sql_connector_version='{dbsql_version}',
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


@dataclass(frozen=True)
class QueryContextWrapper:
    """
    Until dbt tightens this protocol up, we need to wrap the context for safety
    """

    compute_name: Optional[str] = None
    relation_name: Optional[str] = None
    language: Optional[str] = None
    model_unique_id: Optional[str] = None
    model_name: Optional[str] = None

    @staticmethod
    def from_context(query_header_context: Any) -> "QueryContextWrapper":
        if query_header_context is None:
            return QueryContextWrapper()
        compute_name = None
        language = getattr(query_header_context, "language", None)
        relation_name = getattr(query_header_context, "relation_name", "[unknown]")
        model_unique_id = None
        model_name = None

        if hasattr(query_header_context, "config") and query_header_context.config:
            compute_name = query_header_context.config.get("databricks_compute")

        # Extract model identification from context
        if hasattr(query_header_context, "unique_id"):
            model_unique_id = query_header_context.unique_id
        if hasattr(query_header_context, "name"):
            model_name = query_header_context.name
        # Fallback: try to extract from relation_name if it contains model info
        elif relation_name and relation_name != "[unknown]":
            model_name = relation_name

        return QueryContextWrapper(
            compute_name=compute_name,
            relation_name=relation_name,
            language=language,
            model_unique_id=model_unique_id,
            model_name=model_name,
        )


class DatabricksMacroQueryStringSetter(MacroQueryStringSetter):
    def _get_comment_macro(self) -> Optional[str]:
        if self.config.query_comment.comment == DEFAULT_QUERY_COMMENT:
            return DATABRICKS_QUERY_COMMENT
        else:
            return self.config.query_comment.comment


@dataclass(init=False)
class DatabricksDBTConnection(Connection):
    last_used_time: Optional[float] = None
    acquire_release_count: int = 0
    compute_name: str = ""
    http_path: str = ""
    model_unique_id: Optional[str] = None
    model_name: Optional[str] = None
    max_idle_time: int = DEFAULT_MAX_IDLE_TIME

    # If the connection is being used for a model we want to track the model language.
    # We do this because we need special handling for python models.  Python models will
    # acquire a connection, but do not actually use it to run the model. This can lead to the
    # session timing out on the back end.  However, when the connection is released we set the
    # last_used_time, essentially indicating that the connection was in use while the python
    # model was running. So the session is not refreshed by idle connection cleanup and errors
    # the next time it is used.
    language: Optional[str] = None

    session_id: Optional[str] = None

    def _acquire(self, query_header_context: QueryContextWrapper) -> None:
        """Indicate that this connection is in use."""

        self.acquire_release_count += 1
        if self.last_used_time is None:
            self.last_used_time = time.time()
        self.language = query_header_context.language
        # Update model identification
        if query_header_context.model_unique_id:
            self.model_unique_id = query_header_context.model_unique_id
        if query_header_context.model_name:
            self.model_name = query_header_context.model_name

    def _release(self) -> None:
        """Indicate that this connection is not in use."""
        # Need to check for > 0 because in some situations the dbt code will make an extra
        # release call on a connection.
        if self.acquire_release_count > 0:
            self.acquire_release_count -= 1

        # We don't update the last_used_time for python models because the python model
        # is submitted through a different mechanism and doesn't actually use the connection.
        if self.acquire_release_count == 0 and self.language != "python":
            self.last_used_time = time.time()

    def _get_idle_time(self) -> float:
        return 0 if self.last_used_time is None else time.time() - self.last_used_time

    def _idle_too_long(self) -> bool:
        return self.max_idle_time > 0 and self._get_idle_time() > self.max_idle_time

    def __str__(self) -> str:
        return (
            f"DatabricksDBTConnection(session-id={self.session_id}, "
            f"name={self.name}, model={self.model_name or self.model_unique_id or 'unknown'}, "
            f"idle-time={self._get_idle_time()}s, language={self.language}, "
            f"compute-name={self.compute_name})"
        )

    def _reset_handle(self, open: Callable[[Connection], Connection]) -> None:
        self.handle = LazyHandle(open)
        self.session_id = None
        # Reset last_used_time to None because by refreshing this connection becomes associated
        # with a new session that hasn't been used yet.
        self.last_used_time = None
        logger.debug(ConnectionReset(str(self)))


class DatabricksConnectionManager(SparkConnectionManager):
    TYPE: str = "databricks"
    credentials_manager: Optional[DatabricksCredentialManager] = None

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext):
        super().__init__(profile, mp_context)
        self._api_client: Optional[DatabricksApiClient] = None

    @property
    def api_client(self) -> DatabricksApiClient:
        if self._api_client is None:
            self._api_client = DatabricksApiClient.create(
                cast(DatabricksCredentials, self.profile.credentials), 15 * 60
            )
        return self._api_client

    def is_cluster(self) -> bool:
        conn = self.get_thread_connection()
        return (
            conn.credentials.cluster_id is not None
            # Credentials field is not updated when overriding the compute at model level.
            # This secondary check is a workaround for that case
            or "/warehouses/" not in cast(DatabricksDBTConnection, conn).http_path
        )

    def cancel_open(self) -> list[str]:
        cancelled = super().cancel_open()
        logger.info("Cancelling open python jobs")
        PythonRunTracker.cancel_runs(self.api_client)
        return cancelled

    def compare_dbr_version(self, major: int, minor: int) -> int:
        version = (major, minor)

        handle: DatabricksHandle = self.get_thread_connection().handle
        dbr_version = handle.dbr_version
        return (dbr_version > version) - (dbr_version < version)

    def set_query_header(self, query_header_context: dict[str, Any]) -> None:
        self.query_header = DatabricksMacroQueryStringSetter(self.profile, query_header_context)

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        log_sql = redact_credentials(sql)

        try:
            yield

        except Error as exc:
            logger.debug(QueryError(log_sql, exc))
            raise DbtDatabaseError(str(exc)) from exc

        except Exception as exc:
            logger.debug(QueryError(log_sql, exc))
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise DbtDatabaseError(msg) from exc
            else:
                raise DbtDatabaseError(str(exc)) from exc

    # override/overload
    def set_connection_name(
        self, name: Optional[str] = None, query_header_context: Any = None
    ) -> Connection:
        """Called by 'acquire_connection' in DatabricksAdapter, which is called by
        'connection_named', called by 'connection_for(node)'.
        Creates a fresh connection per model, reusing within the same model execution."""

        conn_name: str = "master" if name is None else name
        wrapped = QueryContextWrapper.from_context(query_header_context)

        # Check if we already have a connection for this thread
        existing_conn = self.get_if_exists()

        if existing_conn is not None:
            # Reuse existing connection for the same model execution
            # Cast to our specific connection type
            databricks_conn = cast(DatabricksDBTConnection, existing_conn)
            # Just update the name if needed and return
            if databricks_conn.name != conn_name:
                databricks_conn.name = conn_name
            databricks_conn._acquire(wrapped)
            return databricks_conn

        # No existing connection, create a fresh one
        conn = self._create_fresh_connection(conn_name, wrapped)

        # Store in thread-based storage (handled by parent SparkConnectionManager)
        self.set_thread_connection(conn)

        # Acquire the connection for immediate use
        conn._acquire(wrapped)

        return conn

    def add_begin_query(self) -> Any:
        return (None, None)

    def add_commit_query(self) -> Any:
        return (None, None)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: tuple[type[Exception], ...] = tuple(),
        retry_limit: int = 1,
        *,
        close_cursor: bool = False,
    ) -> tuple[Connection, Any]:
        connection = self.get_thread_connection()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=cast_to_str(connection.name)))

        with self.exception_handler(sql):
            cursor: Optional[CursorWrapper] = None
            try:
                log_sql = redact_credentials(sql)
                if abridge_sql_log:
                    log_sql = f"{log_sql[:512]}..."

                fire_event(
                    SQLQuery(
                        conn_name=cast_to_str(connection.name),
                        sql=log_sql,
                        node_info=get_node_info(),
                    )
                )

                pre = time.time()

                handle: DatabricksHandle = connection.handle
                cursor = handle.execute(sql, bindings)

                fire_event(
                    SQLQueryStatus(
                        status=str(cursor.get_response()),
                        elapsed=round((time.time() - pre), 2),
                        node_info=get_node_info(),
                    )
                )

                return connection, cursor
            except Error:
                close_cursor = True
                raise
            finally:
                if close_cursor and cursor is not None:
                    cursor.close()

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> tuple[AdapterResponse, "Table"]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = cursor.get_response()
            if fetch:
                table = self.get_result_from_cursor(cursor, limit)
            else:
                # Lazy import agate to improve CLI startup time
                from dbt_common.clients import agate_helper

                table = agate_helper.empty_table()
            return response, table
        finally:
            cursor.close()

    def _execute_with_cursor(
        self, log_sql: str, f: Callable[[DatabricksHandle], CursorWrapper]
    ) -> "Table":
        connection = self.get_thread_connection()

        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=cast_to_str(connection.name)))

        with self.exception_handler(log_sql):
            cursor: Optional[CursorWrapper] = None
            try:
                fire_event(
                    SQLQuery(
                        conn_name=cast_to_str(connection.name),
                        sql=log_sql,
                        node_info=get_node_info(),
                    )
                )

                pre = time.time()

                handle: DatabricksHandle = connection.handle
                cursor = f(handle)

                fire_event(
                    SQLQueryStatus(
                        status=str(self.get_response(cursor)),
                        elapsed=round((time.time() - pre), 2),
                        node_info=get_node_info(),
                    )
                )

                return self.get_result_from_cursor(cursor, None)
            finally:
                if cursor:
                    cursor.close()

    def list_schemas(self, database: str, schema: Optional[str] = None) -> "Table":
        database = database.strip("`")
        if schema:
            schema = schema.strip("`").lower()
        return self._execute_with_cursor(
            f"GetSchemas(database={database}, schema={schema})",
            lambda cursor: cursor.list_schemas(database=database, schema=schema),
        )

    def list_tables(self, database: str, schema: str) -> "Table":
        database = database.strip("`")
        schema = schema.strip("`").lower()
        return self._execute_with_cursor(
            f"GetTables(database={database}, schema={schema})",
            lambda cursor: cursor.list_tables(database=database, schema=schema),
        )

    # override
    def release(self) -> None:
        with self.lock:
            conn = cast(Optional[DatabricksDBTConnection], self.get_if_exists())
            if conn is None:
                return

        conn._release()

    # override
    def cleanup_all(self) -> None:
        with self.lock:
            # Clean up any remaining thread connections (for backward compatibility)
            self.thread_connections.clear()

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        databricks_connection = cast(DatabricksDBTConnection, connection)

        if connection.state == ConnectionState.OPEN:
            return connection

        creds: DatabricksCredentials = connection.credentials
        timeout = creds.connect_timeout

        cls.credentials_manager = creds.authenticate()
        conn_args = SqlUtils.prepare_connection_arguments(
            creds, cls.credentials_manager, databricks_connection.http_path
        )

        def connect() -> DatabricksHandle:
            try:
                # TODO: what is the error when a user specifies a catalog they don't have access to
                conn = DatabricksHandle.from_connection_args(
                    conn_args, creds.cluster_id is not None
                )
                if conn:
                    databricks_connection.session_id = conn.session_id
                    databricks_connection.last_used_time = time.time()
                    return conn
                else:
                    raise DbtDatabaseError("Failed to create connection")
            except Error as exc:
                logger.error(ConnectionCreateError(exc))
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

    # override
    @classmethod
    def close(cls, connection: Connection) -> Connection:
        try:
            return super().close(connection)
        except Exception as e:
            logger.warning(f"ignoring error when closing connection: {e}")
            connection.state = ConnectionState.CLOSED
            return connection

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        if isinstance(cursor, CursorWrapper):
            return cursor.get_response()
        else:
            return AdapterResponse("OK")

    def clear_transaction(self) -> None:
        """Noop."""
        pass

    def commit_if_has_connection(self) -> None:
        """Noop."""
        pass

    def get_thread_connection(self) -> Connection:
        conn = super().get_thread_connection()
        self._cleanup_idle_connections()

        return conn

    def _create_fresh_connection(
        self, conn_name: str, query_header_context: QueryContextWrapper
    ) -> DatabricksDBTConnection:
        """Create a new connection for the combination of model and compute associated
        with the given node."""

        # Create a new connection
        compute_name = query_header_context.compute_name or ""

        conn = DatabricksDBTConnection(
            type=Identifier(self.TYPE),
            name=conn_name,
            state=ConnectionState.INIT,
            transaction_open=False,
            handle=None,
            credentials=self.profile.credentials,
        )
        conn.compute_name = compute_name
        creds = cast(DatabricksCredentials, self.profile.credentials)
        conn.http_path = QueryConfigUtils.get_http_path(query_header_context, creds)
        # Set model identification from context
        conn.model_unique_id = query_header_context.model_unique_id
        conn.model_name = query_header_context.model_name
        conn.max_idle_time = QueryConfigUtils.get_max_idle_time(query_header_context, creds)

        conn.handle = LazyHandle(self.open)

        logger.debug(ConnectionCreate(str(conn)))

        return conn

    def _cleanup_idle_connections(self) -> None:
        """Simplified cleanup - just clear thread connections since we create fresh ones per model."""
        # In the simplified model, we don't need complex idle cleanup
        # since connections are created fresh per model and closed when done
        pass

    def cleanup_model_connections(self, model_unique_id: Optional[str]) -> None:
        """Clean up connections for a specific model when it's complete."""
        # In the simplified model, just clear the thread connection
        with self.lock:
            conn = self.get_if_exists()
            if conn:
                logger.debug(f"Cleaning up connection for completed model: {model_unique_id}")
                self.close(conn)
                self.clear_thread_connection()

    def release_model_connection(self, model_unique_id: Optional[str]) -> None:
        """Release a specific model connection."""
        conn = self.get_if_exists()
        if conn and isinstance(conn, DatabricksDBTConnection):
            conn._release()
            logger.debug(f"Released connection for model {model_unique_id}")

    # Note: All connection methods now use simple thread-local storage
    # This ensures that within a single model execution (single thread),
    # all operations use the same connection that gets created fresh and closed when done.


class QueryConfigUtils:
    """
    Utility class for getting config values from QueryHeaderContextWrapper and Credentials.
    """

    @staticmethod
    def get_http_path(context: QueryContextWrapper, creds: DatabricksCredentials) -> str:
        """
        Get the http_path for the compute specified for the node.
        If none is specified default will be used.
        """

        if not context.compute_name:
            return creds.http_path or ""

        # Get the http_path for the named compute.
        http_path = None
        if creds.compute:
            http_path = creds.compute.get(context.compute_name, {}).get("http_path", None)

        # no http_path for the named compute resource is an error condition
        if not http_path:
            raise DbtRuntimeError(
                f"Compute resource {context.compute_name} does not exist or "
                f"does not specify http_path, relation: {context.relation_name}"
            )

        return http_path

    @staticmethod
    def get_max_idle_time(context: QueryContextWrapper, creds: DatabricksCredentials) -> int:
        """Get the http_path for the compute specified for the node.
        If none is specified default will be used."""

        max_idle_time = (
            DEFAULT_MAX_IDLE_TIME if creds.connect_max_idle is None else creds.connect_max_idle
        )

        if context.compute_name and creds.compute:
            max_idle_time = creds.compute.get(context.compute_name, {}).get(
                "connect_max_idle", max_idle_time
            )

        if not isinstance(max_idle_time, int):
            if isinstance(max_idle_time, str) and max_idle_time.strip().isnumeric():
                return int(max_idle_time.strip())
            else:
                raise DbtRuntimeError(
                    f"{max_idle_time} is not a valid value for connect_max_idle. "
                    "Must be a number of seconds."
                )

        return max_idle_time
