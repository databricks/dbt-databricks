import decimal
import os
import re
import sys
import time
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing.context import SpawnContext
from threading import get_ident
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Hashable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

import databricks.sql as dbsql
from agate import Table
from databricks.sql.client import Connection as DatabricksSQLConnection
from databricks.sql.client import Cursor as DatabricksSQLCursor
from databricks.sql.exc import Error
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.contracts.connection import DEFAULT_QUERY_COMMENT
from dbt.adapters.contracts.connection import Identifier
from dbt.adapters.contracts.connection import LazyHandle
from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.auth import BearerAuth
from dbt.adapters.databricks.connection.connection_utils import DEFAULT_MAX_IDLE_TIME
from dbt.adapters.databricks.connection.connection_utils import get_compute_name
from dbt.adapters.databricks.connection.connection_utils import get_max_idle_time
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.credentials import TCredentialProvider
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.utils import redact_credentials
from dbt.adapters.events.types import ConnectionClosedInCleanup
from dbt.adapters.events.types import ConnectionLeftOpenInCleanup
from dbt.adapters.events.types import ConnectionReused
from dbt.adapters.events.types import ConnectionUsed
from dbt.adapters.events.types import NewConnection
from dbt.adapters.events.types import SQLQuery
from dbt.adapters.events.types import SQLQueryStatus
from dbt.adapters.spark.connections import SparkConnectionManager
from dbt_common.clients import agate_helper
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtInternalError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import cast_to_str
from requests import Session

mv_refresh_regex = re.compile(r"refresh\s+materialized\s+view\s+([`\w.]+)", re.IGNORECASE)
st_refresh_regex = re.compile(
    r"create\s+or\s+refresh\s+streaming\s+table\s+([`\w.]+)", re.IGNORECASE
)


DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")


def _log_conn_info(conn: Connection, caller: Optional[str] = None) -> None:
    if isinstance(conn, DatabricksDBTConnection):
        logger.debug(f"conn: {id(conn)}: {caller} {conn._get_conn_info_str()}")
    else:
        logger.debug(f"conn: {id(conn)}: {caller}")


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

    def fetchmany(self, size: int) -> Sequence[Tuple]:
        return self._cursor.fetchmany(size)

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
        headers = self._cursor.connection.thrift_backend._auth_provider._header_factory

        session = Session()
        session.auth = BearerAuth(headers)
        session.headers = {"User-Agent": self._user_agent}

        pipeline_id = _get_table_view_pipeline_id(session, host, model_name)
        pipeline = _get_pipeline_state(session, host, pipeline_id)
        # get the most recently created update for the pipeline
        latest_update = _find_update(pipeline)
        if not latest_update:
            raise DbtRuntimeError(f"No update created for pipeline: {pipeline_id}")

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

            pipeline = _get_pipeline_state(session, host, pipeline_id)
            # get the update we are currently polling
            update = _find_update(pipeline, update_id)
            if not update:
                raise DbtRuntimeError(
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
                msg = _get_update_error_msg(session, host, pipeline_id, update_id)
                if msg:
                    logger.error(msg)

                # another update may have been created due to retry_on_fail settings
                # get the latest update and see if it is a new one
                latest_update = _find_update(pipeline)
                if not latest_update:
                    raise DbtRuntimeError(f"No update created for pipeline: {pipeline_id}")

                latest_update_id = latest_update.get("update_id", "")
                if latest_update_id != update_id:
                    update_id = latest_update_id
                    state = None

        if exceeded_timeout:
            raise DbtRuntimeError("timed out waiting for materialized view refresh")

        if state == "FAILED":
            msg = _get_update_error_msg(session, host, pipeline_id, update_id)
            raise DbtRuntimeError(f"error refreshing model {model_name} {msg}")

        if state == "CANCELED":
            raise DbtRuntimeError(f"refreshing model {model_name} cancelled")

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
        if isinstance(value, decimal.Decimal):
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


@dataclass(init=False)
class DatabricksDBTConnection(Connection):
    last_used_time: Optional[float] = None
    acquire_release_count: int = 0
    compute_name: str = ""
    http_path: str = ""
    thread_identifier: Tuple[int, int] = (0, 0)
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

    def _acquire(self, query_header_context: Any) -> None:
        """Indicate that this connection is in use."""
        self._log_usage(query_header_context)
        self.acquire_release_count += 1
        if self.last_used_time is None:
            self.last_used_time = time.time()
        if query_header_context and hasattr(query_header_context, "language"):
            self.language = query_header_context.language
        else:
            self.language = None

        _log_conn_info(self, "_acquire")

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

        _log_conn_info(self, "_release")

    def _get_idle_time(self) -> float:
        return 0 if self.last_used_time is None else time.time() - self.last_used_time

    def _idle_too_long(self) -> bool:
        return self.max_idle_time > 0 and self._get_idle_time() > self.max_idle_time

    def _get_conn_info_str(self) -> str:
        """Generate a string describing this connection."""
        return (
            f"sess: {self.session_id}, name: {self.name}, "
            f"idle: {self._get_idle_time()}s, acqrelcnt: {self.acquire_release_count}, "
            f"lang: {self.language}, thrd: {self.thread_identifier}, "
            f"cmpt: `{self.compute_name}`, lut: {self.last_used_time}"
        )

    def _log_usage(self, query_header_context: Any) -> None:
        if query_header_context:
            # ResultNode *should* have relation_name attr, but we work around a core
            # issue by checking.
            relation_name = getattr(query_header_context, "relation_name", "[unknown]")
            if not self.compute_name:
                logger.debug(
                    f"On thread {self.thread_identifier}: {relation_name} "
                    "using default compute resource."
                )
            else:
                logger.debug(
                    f"On thread {self.thread_identifier}: {relation_name} "
                    f"using compute resource '{self.compute_name}'."
                )
        else:
            logger.debug(f"Thread {self.thread_identifier} using default compute resource.")

    def _reset_handle(self, open: Callable[[Connection], Connection]) -> None:
        _log_conn_info(self, "_reset_handle")
        self.handle = LazyHandle(open)
        self.session_id = None
        # Reset last_used_time to None because by refreshing this connection becomes associated
        # with a new session that hasn't been used yet.
        self.last_used_time = None


class ExtendedSessionConnectionManager(SparkConnectionManager):
    TYPE: str = "databricks"
    credentials_provider: Optional[TCredentialProvider] = None

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext) -> None:
        super().__init__(profile, mp_context)
        self.threads_compute_connections: Dict[
            Hashable, Dict[Hashable, DatabricksDBTConnection]
        ] = {}

    def compare_dbr_version(self, major: int, minor: int) -> int:
        version = (major, minor)

        connection: DatabricksSQLConnectionWrapper = self.get_thread_connection().handle
        dbr_version = connection.dbr_version
        return (dbr_version > version) - (dbr_version < version)

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        self.query_header = DatabricksMacroQueryStringSetter(self.profile, query_header_context)

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        log_sql = redact_credentials(sql)

        try:
            yield

        except Error as exc:
            logger.debug(f"Error while running:\n{log_sql}")
            _log_dbsql_errors(exc)
            raise DbtRuntimeError(str(exc)) from exc

        except Exception as exc:
            logger.debug(f"Error while running:\n{log_sql}")
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise DbtRuntimeError(msg) from exc
            else:
                raise DbtRuntimeError(str(exc)) from exc

    def set_connection_name(
        self, name: Optional[str] = None, query_header_context: Any = None
    ) -> Connection:
        """Called by 'acquire_connection' in DatabricksAdapter, which is called by
        'connection_named', called by 'connection_for(node)'.
        Creates a connection for this thread if one doesn't already
        exist, and will rename an existing connection."""

        self._cleanup_idle_connections()

        conn_name: str = "master" if name is None else name

        # Get a connection for this thread
        conn = self._get_if_exists_compute_connection(get_compute_name(query_header_context) or "")

        if conn is None:
            conn = self._create_compute_connection(conn_name, query_header_context)
        else:  # existing connection either wasn't open or didn't have the right name
            conn = self._update_compute_connection(conn, conn_name)

        conn._acquire(query_header_context)

        return conn

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

                fire_event(
                    SQLQuery(
                        conn_name=cast_to_str(connection.name),
                        sql=log_sql,
                        node_info=get_node_info(),
                    )
                )

                pre = time.time()

                cursor = cast(DatabricksSQLConnectionWrapper, connection.handle).cursor()
                cursor.execute(sql, bindings)

                fire_event(
                    SQLQueryStatus(
                        status=str(self.get_response(cursor)),
                        elapsed=round((time.time() - pre), 2),
                        node_info=get_node_info(),
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
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
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
                fire_event(
                    SQLQuery(
                        conn_name=cast_to_str(connection.name),
                        sql=log_sql,
                        node_info=get_node_info(),
                    )
                )

                pre = time.time()

                handle: DatabricksSQLConnectionWrapper = connection.handle
                cursor = handle.cursor()
                f(cursor)

                fire_event(
                    SQLQueryStatus(
                        status=str(self.get_response(cursor)),
                        elapsed=round((time.time() - pre), 2),
                        node_info=get_node_info(),
                    )
                )

                return self.get_result_from_cursor(cursor, None)
            finally:
                if cursor is not None:
                    cursor.close()

    def list_schemas(self, database: str, schema: Optional[str] = None) -> Table:
        database = database.strip("`")
        if schema:
            schema = schema.strip("`").lower()
        return self._execute_cursor(
            f"GetSchemas(database={database}, schema={schema})",
            lambda cursor: cursor.schemas(catalog_name=database, schema_name=schema),
        )

    def list_tables(self, database: str, schema: str, identifier: Optional[str] = None) -> Table:
        database = database.strip("`")
        schema = schema.strip("`").lower()
        if identifier:
            identifier = identifier.strip("`")
        return self._execute_cursor(
            f"GetTables(database={database}, schema={schema}, identifier={identifier})",
            lambda cursor: cursor.tables(
                catalog_name=database, schema_name=schema, table_name=identifier
            ),
        )

    # override
    def release(self) -> None:
        with self.lock:
            conn = cast(Optional[DatabricksDBTConnection], self.get_if_exists())
            if conn is None:
                return

        conn._release()

    # override
    @classmethod
    def close(cls, connection: Connection) -> Connection:
        try:
            return super().close(connection)
        except Exception as e:
            logger.warning(f"ignoring error when closing connection: {e}")
            connection.state = ConnectionState.CLOSED
            return connection

    # override
    def cleanup_all(self) -> None:
        with self.lock:
            for thread_connections in self.threads_compute_connections.values():
                for connection in thread_connections.values():
                    if connection.acquire_release_count > 0:
                        fire_event(
                            ConnectionLeftOpenInCleanup(conn_name=cast_to_str(connection.name))
                        )
                    else:
                        fire_event(
                            ConnectionClosedInCleanup(conn_name=cast_to_str(connection.name))
                        )
                    self.close(connection)

            # garbage collect these connections
            self.thread_connections.clear()
            self.threads_compute_connections.clear()

    def _update_compute_connection(
        self, conn: DatabricksDBTConnection, new_name: str
    ) -> DatabricksDBTConnection:

        if conn.name == new_name and conn.state == ConnectionState.OPEN:
            # Found a connection and nothing to do, so just return it
            return conn

        orig_conn_name: str = conn.name or ""

        if conn.state != ConnectionState.OPEN:
            conn.handle = LazyHandle(self.open)
        if conn.name != new_name:
            conn.name = new_name
            fire_event(ConnectionReused(orig_conn_name=orig_conn_name, conn_name=new_name))

        current_thread_conn = cast(Optional[DatabricksDBTConnection], self.get_if_exists())
        if current_thread_conn and current_thread_conn.compute_name != conn.compute_name:
            self.clear_thread_connection()
            self.set_thread_connection(conn)

        _log_conn_info(conn, f"reusing connection {orig_conn_name}")

        return conn

    def _add_compute_connection(self, conn: DatabricksDBTConnection) -> None:
        """Add a new connection to the map of connection per thread per compute."""

        with self.lock:
            thread_map = self._get_compute_connections()
            if conn.compute_name in thread_map:
                raise DbtInternalError(
                    f"In set_thread_compute_connection, connection exists for `{conn.compute_name}`"
                )
            thread_map[conn.compute_name] = conn

    def _get_compute_connections(
        self,
    ) -> Dict[Hashable, DatabricksDBTConnection]:
        """Retrieve a map of compute name to connection for the current thread."""

        thread_id = self.get_thread_identifier()
        with self.lock:
            thread_map = self.threads_compute_connections.get(thread_id)
            if not thread_map:
                thread_map = {}
                self.threads_compute_connections[thread_id] = thread_map
            return thread_map

    def _get_if_exists_compute_connection(
        self, compute_name: str
    ) -> Optional[DatabricksDBTConnection]:
        """Get the connection for the current thread and named compute, if it exists."""

        with self.lock:
            threads_map = self._get_compute_connections()
            return threads_map.get(compute_name)

    def _cleanup_idle_connections(self) -> None:
        with self.lock:
            # Get all connections associated with this thread. There can be multiple connections
            # if different models use different compute resources
            thread_conns = self._get_compute_connections()
            for conn in thread_conns.values():
                _log_conn_info(conn, "idle check connection:")

                # Generally speaking we only want to close/refresh the connection if the
                # acquire_release_count is zero.  i.e. the connection is not currently in use.
                # However python models acquire a connection then run the pyton model, which
                # doesn't actually use the connection. If the python model takes lone enought to
                # run the connection can be idle long enough to timeout on the back end.
                # If additional sql needs to be run after the python model, but before the
                # connection is released, the connection needs to be refreshed or there will
                # be a failure.  Making an exception when language is 'python' allows the
                # the call to _cleanup_idle_connections from get_thread_connection to refresh the
                # connection in this scenario.
                if (
                    conn.acquire_release_count == 0 or conn.language == "python"
                ) and conn._idle_too_long():
                    _log_conn_info(conn, "closing idle connection")
                    self.close(conn)
                    conn._reset_handle(self.open)

    def _create_compute_connection(
        self, conn_name: str, query_header_context: Any = None
    ) -> DatabricksDBTConnection:
        """Create anew connection for the combination of current thread and compute associated
        with the given node."""

        # Create a new connection
        compute_name = get_compute_name(query_header_context) or ""

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
        conn.http_path = _get_http_path(query_header_context, creds=creds) or ""
        conn.thread_identifier = cast(Tuple[int, int], self.get_thread_identifier())
        conn.max_idle_time = get_max_idle_time(creds, compute_name)

        conn.handle = LazyHandle(self.open)

        _log_conn_info(conn, "Creating DatabricksDBTConnection")

        # Add this connection to the thread/compute connection pool.
        self._add_compute_connection(conn)
        # Remove the connection currently in use by this thread from the thread connection pool.
        self.clear_thread_connection()
        # Add the connection to thread connection pool.
        self.set_thread_connection(conn)

        fire_event(
            NewConnection(conn_name=conn_name, conn_type=self.TYPE, node_info=get_node_info())
        )

        return conn

    def get_thread_connection(self) -> Connection:
        self._cleanup_idle_connections()
        conn = super().get_thread_connection()
        _log_conn_info(conn, "get_thread_connection:")

        return conn

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        databricks_connection = cast(DatabricksDBTConnection, connection)

        if connection.state == ConnectionState.OPEN:
            _log_conn_info(databricks_connection, "Connection is already open, skipping open.")
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

        # If a model specifies a compute resource the http path
        # may be different than the http_path property of creds.
        http_path = databricks_connection.http_path

        def connect() -> DatabricksSQLConnectionWrapper:
            try:
                # TODO: what is the error when a user specifies a catalog they don't have access to
                conn: DatabricksSQLConnection = dbsql.connect(
                    server_hostname=creds.host,
                    http_path=http_path,
                    credentials_provider=cls.credentials_provider,
                    http_headers=http_headers if http_headers else None,
                    session_configuration=creds.session_properties,
                    catalog=creds.database,
                    use_inline_params="silent",
                    # schema=creds.schema,  # TODO: Explicitly set once DBR 7.3LTS is EOL.
                    _user_agent_entry=user_agent_entry,
                    **connection_parameters,
                )

                if conn:
                    databricks_connection.session_id = conn.get_session_id_hex()
                databricks_connection.last_used_time = time.time()
                _log_conn_info(databricks_connection, "session opened")

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
    refresh_search = mv_refresh_regex.search(sql)
    if not refresh_search:
        refresh_search = st_refresh_regex.search(sql)

    if refresh_search:
        name = refresh_search.group(1).replace("`", "")

    return refresh_search is not None, name


def _get_table_view_pipeline_id(session: Session, host: str, name: str) -> str:
    table_url = f"https://{host}/api/2.1/unity-catalog/tables/{name}"
    resp1 = session.get(table_url)
    if resp1.status_code != 200:
        raise DbtRuntimeError(
            f"Error getting info for materialized view/streaming table {name}: {resp1.text}"
        )

    pipeline_id = resp1.json().get("pipeline_id", "")
    if not pipeline_id:
        raise DbtRuntimeError(
            f"Materialized view/streaming table {name} does not have a pipeline id"
        )

    return pipeline_id


def _get_pipeline_state(session: Session, host: str, pipeline_id: str) -> dict:
    pipeline_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}"

    response = session.get(pipeline_url)
    if response.status_code != 200:
        raise DbtRuntimeError(f"Error getting pipeline info for {pipeline_id}: {response.text}")

    return response.json()


def _find_update(pipeline: dict, id: str = "") -> Optional[Dict]:
    updates = pipeline.get("latest_updates", [])
    if not updates:
        raise DbtRuntimeError(f"No updates for pipeline: {pipeline.get('pipeline_id', '')}")

    if not id:
        return updates[0]

    matches = [x for x in updates if x.get("update_id") == id]
    if matches:
        return matches[0]

    return None


def _get_update_error_msg(session: Session, host: str, pipeline_id: str, update_id: str) -> str:
    events_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}/events"
    response = session.get(events_url)
    if response.status_code != 200:
        raise DbtRuntimeError(
            f"Error getting pipeline event info for {pipeline_id}: {response.text}"
        )

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


def _get_http_path(query_header_context: Any, creds: DatabricksCredentials) -> Optional[str]:
    """Get the http_path for the compute specified for the node.
    If none is specified default will be used."""

    # ResultNode *should* have relation_name attr, but we work around a core
    # issue by checking.
    relation_name = getattr(query_header_context, "relation_name", "[unknown]")

    # If there is no node we return the http_path for the default compute.
    if not query_header_context:
        return creds.http_path

    # Get the name of the compute resource specified in the node's config.
    # If none is specified return the http_path for the default compute.
    compute_name = get_compute_name(query_header_context)
    if not compute_name:
        return creds.http_path

    # Get the http_path for the named compute.
    http_path = None
    if creds.compute:
        http_path = creds.compute.get(compute_name, {}).get("http_path", None)

    # no http_path for the named compute resource is an error condition
    if not http_path:
        raise DbtRuntimeError(
            f"Compute resource {compute_name} does not exist or "
            f"does not specify http_path, relation: {relation_name}"
        )

    return http_path
