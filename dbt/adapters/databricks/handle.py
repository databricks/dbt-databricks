import decimal
import re
import sys
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from dbt_common.exceptions import DbtRuntimeError

import databricks.sql as dbsql
from databricks.sql.client import Connection, Cursor
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.credentials import DatabricksCredentialManager, DatabricksCredentials
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    pass


CursorOp = Callable[[Cursor], None]
CursorExecOp = Callable[[Cursor], Cursor]
CursorWrapperOp = Callable[["CursorWrapper"], None]
ConnectionOp = Callable[[Optional[Connection]], None]
LogOp = Callable[[], str]
FailLogOp = Callable[[Exception], str]


class CursorWrapper:
    """
    Wrap the DBSQL cursor to abstract the details from DatabricksConnectionManager.
    """

    def __init__(self, cursor: Cursor):
        self._cursor = cursor
        self.open = True

    @property
    def description(self) -> Optional[list[tuple]]:
        return self._cursor.description

    def cancel(self) -> None:
        if self._cursor.active_op_handle:
            self._cleanup(
                lambda cursor: cursor.cancel(),
                lambda: f"{self} - Cancelling",
                lambda ex: f"{self} - Exception while cancelling: {ex}",
            )

    def close(self) -> None:
        self._cleanup(
            lambda cursor: cursor.close(),
            lambda: f"{self} - Closing",
            lambda ex: f"{self} - Exception while closing: {ex}",
        )

    def _cleanup(
        self,
        cleanup: CursorOp,
        startLog: LogOp,
        failLog: FailLogOp,
    ) -> None:
        """
        Common cleanup function for cursor operations, handling either close or cancel.
        """
        if self.open:
            self.open = False
            logger.debug(startLog())
            utils.handle_exceptions_as_warning(lambda: cleanup(self._cursor), failLog)

    def fetchall(self) -> Sequence[tuple]:
        return self._safe_execute(lambda cursor: cursor.fetchall())

    def fetchone(self) -> Optional[tuple]:
        return self._safe_execute(lambda cursor: cursor.fetchone())

    def fetchmany(self, size: int) -> Sequence[tuple]:
        return self._safe_execute(lambda cursor: cursor.fetchmany(size))

    def get_response(self) -> AdapterResponse:
        return AdapterResponse(_message="OK", query_id=self._cursor.query_id or "N/A")

    T = TypeVar("T")

    def _safe_execute(self, f: Callable[[Cursor], T]) -> T:
        if not self.open:
            raise DbtRuntimeError("Attempting to execute on a closed cursor")
        return f(self._cursor)

    def __str__(self) -> str:
        return (
            f"Cursor(session-id={self._cursor.connection.get_session_id_hex()}, "
            f"command-id={self._cursor.query_id})"
        )

    def __enter__(self) -> "CursorWrapper":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return exc_val is None


class DatabricksHandle:
    """
    Handle for a Databricks SQL Session.
    Provides a layer of abstraction over the Databricks SQL client library such that
    DatabricksConnectionManager does not depend on the details of this library directly.
    """

    def __init__(
        self,
        conn: Connection,
        is_cluster: bool,
    ):
        self._conn = conn
        self.open = True
        self._cursor: Optional[CursorWrapper] = None
        self._dbr_version: Optional[tuple[int, int]] = None
        self._is_cluster = is_cluster

    @property
    def dbr_version(self) -> tuple[int, int]:
        """
        Gets the DBR version of the current session.
        """
        if not self._dbr_version:
            if self._is_cluster:
                cursor = self._safe_execute(
                    lambda cursor: cursor.execute(
                        "SET spark.databricks.clusterUsageTags.sparkVersion"
                    )
                )
                results = cursor.fetchone()
                self._dbr_version = SqlUtils.extract_dbr_version(results[1] if results else "")
                cursor.close()
            else:
                # Assuming SQL Warehouse uses the latest version.
                self._dbr_version = (sys.maxsize, sys.maxsize)

        return self._dbr_version

    @property
    def session_id(self) -> str:
        return self._conn.get_session_id_hex()

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> CursorWrapper:
        """
        Execute a SQL statement on the current session with optional bindings.
        """
        return self._safe_execute(
            lambda cursor: cursor.execute(
                SqlUtils.clean_sql(sql), SqlUtils.translate_bindings(bindings)
            )
        )

    def list_schemas(self, database: str, schema: Optional[str] = None) -> CursorWrapper:
        """
        Get a cursor for listing schemas in the given database.
        """
        return self._safe_execute(lambda cursor: cursor.schemas(database, schema))

    def list_tables(self, database: str, schema: str) -> "CursorWrapper":
        """
        Get a cursor for listing tables in the given database and schema.
        """

        return self._safe_execute(lambda cursor: cursor.tables(database, schema))

    def cancel(self) -> None:
        """
        Cancel in progress query, if any, then close connection and cursor.
        """
        self._cleanup(
            lambda cursor: cursor.cancel(),
            lambda: f"{self} - Cancelling",
            lambda ex: f"{self} - Exception while cancelling: {ex}",
        )

    def close(self) -> None:
        """
        Close the connection and cursor.
        """

        self._cleanup(
            lambda cursor: cursor.close(),
            lambda: f"{self} - Closing",
            lambda ex: f"{self} - Exception while closing: {ex}",
        )

    def rollback(self) -> None:
        """
        Required for interface compatibility, but not implemented.
        """
        logger.debug("NotImplemented: rollback")

    @staticmethod
    def from_connection_args(
        conn_args: dict[str, Any], is_cluster: bool
    ) -> Optional["DatabricksHandle"]:
        """
        Create a new DatabricksHandle from the given connection arguments.
        """

        conn = dbsql.connect(**conn_args)
        if not conn:
            logger.warning(f"Failed to create connection for {conn_args.get('http_path')}")
            return None
        connection = DatabricksHandle(conn, is_cluster=is_cluster)
        logger.debug(f"{connection} - Created")

        return connection

    def _cleanup(
        self,
        cursor_op: CursorWrapperOp,
        startLog: LogOp,
        failLog: FailLogOp,
    ) -> None:
        """
        Function for cleaning up the connection and cursor, handling either close or cancel.
        """
        if self.open:
            self.open = False
            logger.debug(startLog())

            if self._cursor:
                cursor_op(self._cursor)

            utils.handle_exceptions_as_warning(lambda: self._conn.close(), failLog)

    def _safe_execute(self, f: CursorExecOp) -> CursorWrapper:
        """
        Ensure that a previously opened cursor is closed and that a new one is created
        before executing the given function.
        Also ensures that we do not continue to execute SQL after a connection cleanup
        has been requested.
        """

        if not self.open:
            raise DbtRuntimeError("Attempting to execute on a closed connection")
        assert self._conn, "Should not be possible for _conn to be None if open"
        if self._cursor:
            self._cursor.close()
        self._cursor = CursorWrapper(f(self._conn.cursor()))
        return self._cursor

    def __del__(self) -> None:
        if self._cursor:
            self._cursor.close()

        self.close()

    def __str__(self) -> str:
        return f"Connection(session-id={self.session_id})"


class SqlUtils:
    """
    Utility class for preparing cursor input/output.
    """

    DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")
    user_agent = f"dbt-databricks/{__version__}"

    @staticmethod
    def extract_dbr_version(version: str) -> tuple[int, int]:
        m = SqlUtils.DBR_VERSION_REGEX.search(version)
        if m:
            major = int(m.group(1))
            if m.group(2) == "x":
                minor = sys.maxsize
            else:
                minor = int(m.group(2))
            return (major, minor)
        else:
            raise DbtRuntimeError("Failed to detect DBR version")

    @staticmethod
    def translate_bindings(bindings: Optional[Sequence[Any]]) -> Optional[Sequence[Any]]:
        if bindings:
            return list(map(lambda x: float(x) if isinstance(x, decimal.Decimal) else x, bindings))
        return None

    @staticmethod
    def clean_sql(sql: str) -> str:
        cleaned = sql.strip()
        if cleaned.endswith(";"):
            cleaned = cleaned[:-1]
        return cleaned

    @staticmethod
    def prepare_connection_arguments(
        creds: DatabricksCredentials, creds_manager: DatabricksCredentialManager, http_path: str
    ) -> dict[str, Any]:
        invocation_env = creds.get_invocation_env()
        user_agent_entry = SqlUtils.user_agent
        if invocation_env:
            user_agent_entry += f"; {invocation_env}"

        connection_parameters = creds.connection_parameters.copy()  # type: ignore[union-attr]

        http_headers: list[tuple[str, str]] = list(
            creds.get_all_http_headers(connection_parameters.pop("http_headers", {})).items()
        )

        return {
            "server_hostname": creds.host,
            "http_path": http_path,
            "credentials_provider": creds_manager.credentials_provider,
            "http_headers": http_headers if http_headers else None,
            "session_configuration": creds.session_properties,
            "catalog": creds.database,
            "use_inline_params": "silent",
            "schema": creds.schema,
            "_user_agent_entry": user_agent_entry,
            "user_agent_entry": user_agent_entry,
            **connection_parameters,
        }
