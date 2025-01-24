import decimal
import re
import sys
from collections.abc import Callable, Sequence
from functools import partial
from threading import RLock
from typing import TYPE_CHECKING, Any, Optional

from dbt_common.exceptions import DbtRuntimeError

from databricks.sql.client import Connection, Cursor
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    pass


CursorOp = Callable[[Cursor], None]
CursorExecOp = Callable[[Cursor], Cursor]
CursorWrapperOp = Callable[["CursorWrapper"], None]
ConnectionOp = Callable[[Connection], None]
LogOp = Callable[[], str]
FailLogOp = Callable[[Exception], str]


class CursorWrapper:
    def __init__(self, cursor: Cursor):
        self._cursor = cursor
        self.open = True

    @property
    def description(self) -> Optional[list[tuple]]:
        return self._cursor.description

    def cancel(self) -> None:
        if self._cursor.active_op_handle:
            self._cleanup(
                Cursor.cancel,
                lambda: f"{self} - Cancelling",
                lambda ex: f"{self} - Exception while cancelling: {ex}",
            )

    def close(self) -> None:
        self._cleanup(
            Cursor.close,
            lambda: f"{self} - Closing",
            lambda ex: f"{self} - Exception while closing: {ex}",
        )

    def _cleanup(
        self,
        cleanup: CursorOp,
        startLog: LogOp,
        failLog: FailLogOp,
    ) -> None:
        if self.open:
            self.open = False
            logger.debug(startLog())
            utils.handle_exceptions_as_warning(lambda: cleanup(self._cursor), failLog)

    def fetchall(self) -> Sequence[tuple]:
        return self._cursor.fetchall()

    def fetchone(self) -> Optional[tuple]:
        return self._cursor.fetchone()

    def fetchmany(self, size: int) -> Sequence[tuple]:
        return self._cursor.fetchmany(size)

    def get_response(self) -> AdapterResponse:
        return AdapterResponse(_message="OK", query_id=self._cursor.query_id or "N/A")

    def __str__(self) -> str:
        return (
            f"Cursor(session-id={self._cursor.connection.get_session_id_hex()}, "
            f"command-id={self._cursor.query_id})"
        )


class DatabricksHandle:
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
        self._lock = RLock()

    @property
    def dbr_version(self) -> tuple[int, int]:
        if not self._dbr_version:
            if self._is_cluster:
                cursor = self._safe_execute(get_dbr_version)
                results = cursor.fetchone()
                self._dbr_version = extract_dbr_version(results[1] if results else "")
                cursor.close()
            else:
                # Assuming SQL Warehouse uses the latest version.
                self._dbr_version = (sys.maxsize, sys.maxsize)

        return self._dbr_version

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> CursorWrapper:
        return self._safe_execute(
            lambda cursor: cursor.execute(clean_sql(sql), translate_bindings(bindings))
        )

    def list_schemas(self, database: str, schema: Optional[str] = None) -> CursorWrapper:
        return self._safe_execute(partial(list_schemas, catalog_name=database, schema_name=schema))

    def list_tables(self, database: str, schema: str) -> "CursorWrapper":
        return self._safe_execute(partial(list_tables, catalog_name=database, schema_name=schema))

    def cancel(self) -> None:
        self._cleanup(
            Connection.close,
            CursorWrapper.cancel,
            lambda: f"{self} - Cancelling",
            lambda ex: f"{self} - Exception while cancelling: {ex}",
        )

    def close(self) -> None:
        self._cleanup(
            Connection.close,
            CursorWrapper.close,
            lambda: f"{self} - Closing",
            lambda ex: f"{self} - Exception while closing: {ex}",
        )

    def rollback(self) -> None:
        logger.debug("NotImplemented: rollback")

    def _cleanup(
        self,
        connect_op: ConnectionOp,
        cursor_op: CursorWrapperOp,
        startLog: LogOp,
        failLog: FailLogOp,
    ) -> None:
        if self.open:
            self.open = False
            logger.debug(startLog())

            if self._cursor:
                cursor_op(self._cursor)

            utils.handle_exceptions_as_warning(lambda: connect_op(self._conn), failLog)

    def _safe_execute(self, f: CursorExecOp) -> CursorWrapper:
        with self._lock:
            if not self.open:
                raise DbtRuntimeError("Attempting to execute on a closed connection")
            if self._cursor:
                self._cursor.close()
            self._cursor = CursorWrapper(f(self._conn.cursor()))
            return self._cursor

    def __del__(self) -> None:
        if self._cursor:
            self._cursor.close()

        self.close()

    def __str__(self) -> str:
        return f"Connection(session-id={self._conn.get_session_id_hex() or 'Unknown'})"


def clean_sql(sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1]
    return cleaned


def get_dbr_version(cursor: Cursor) -> Cursor:
    return cursor.execute("SET spark.databricks.clusterUsageTags.sparkVersion")


def list_schemas(cursor: Cursor, catalog_name: str, schema_name: Optional[str]) -> Cursor:
    return cursor.schemas(catalog_name=catalog_name, schema_name=schema_name)


def list_tables(cursor: Cursor, catalog_name: str, schema_name: str) -> Cursor:
    return cursor.tables(catalog_name=catalog_name, schema_name=schema_name)


DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")


def extract_dbr_version(version: str) -> tuple[int, int]:
    m = DBR_VERSION_REGEX.search(version)
    if m:
        major = int(m.group(1))
        if m.group(2) == "x":
            minor = sys.maxsize
        else:
            minor = int(m.group(2))
        return (major, minor)
    else:
        raise DbtRuntimeError("Failed to detect DBR version")


def translate_bindings(bindings: Optional[Sequence[Any]]) -> Optional[Sequence[Any]]:
    if bindings:
        return list(map(lambda x: float(x) if isinstance(x, decimal.Decimal) else x, bindings))
    return None
