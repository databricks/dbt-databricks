import decimal
import re
import sys
from collections.abc import Callable, Sequence
from threading import RLock
from typing import TYPE_CHECKING, Any, Optional

from dbt_common.exceptions import DbtRuntimeError

from databricks.sql import Error
from databricks.sql.client import Connection, Cursor
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.databricks.events.connection_events import (
    ConnectionCancel,
    ConnectionCancelError,
    ConnectionClose,
    ConnectionCloseError,
)
from dbt.adapters.databricks.events.cursor_events import (
    CursorClose,
    CursorCloseError,
)
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    pass


class CursorWrapper:
    def __init__(self, cursor: Cursor):
        self._cursor = cursor
        self.open = True

    @property
    def description(self) -> Optional[list[tuple]]:
        return self._cursor.description

    def cancel(self) -> None:
        if self.open and self._cursor.active_op_handle:
            self.open = False
            try:
                self._cursor.cancel()
            except Error as exc:
                logger.warning(ConnectionCancelError(self._conn, exc))

    def close(self) -> None:
        if self.open:
            self.open = False
            logger.debug(CursorClose(self._cursor))

            try:
                self._cursor.close()
            except Error as exc:
                logger.warning(CursorCloseError(self._cursor, exc))

    def fetchall(self) -> Sequence[tuple]:
        return self._cursor.fetchall()

    def fetchone(self) -> Optional[tuple]:
        return self._cursor.fetchone()

    def fetchmany(self, size: int) -> Sequence[tuple]:
        return self._cursor.fetchmany(size)

    def get_response(self) -> AdapterResponse:
        return AdapterResponse(_message="OK", query_id=self._cursor.query_id or "N/A")


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
                cursor = self._safe_execute(
                    lambda cursor: cursor.execute(
                        "SET spark.databricks.clusterUsageTags.sparkVersion"
                    )
                )
                results = cursor.fetchone()
                cursor.close()
                self._dbr_version = extract_dbr_version(results[1] if results else "")
            else:
                # Assuming SQL Warehouse uses the latest version.
                self._dbr_version = (sys.maxsize, sys.maxsize)

        return self._dbr_version

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> CursorWrapper:
        return self._safe_execute(
            lambda cursor: cursor.execute(clean_sql(sql), translate_bindings(bindings))
        )

    def list_schemas(self, database: str, schema: Optional[str] = None) -> CursorWrapper:
        return self._safe_execute(
            lambda cursor: cursor.schemas(catalog_name=database, schema_name=schema)
        )

    def list_tables(self, database: str, schema: str) -> "CursorWrapper":
        return self._safe_execute(
            lambda cursor: cursor.tables(catalog_name=database, schema_name=schema)
        )

    def cancel(self) -> None:
        with self._lock:
            if self.open:
                self.open = False
                logger.debug(ConnectionCancel(self._conn))

                if self._cursor:
                    self._cursor.cancel()

    def close(self) -> None:
        with self._lock:
            if self.open:
                self.open = False
                logger.debug(ConnectionClose(self._conn))

                try:
                    self._conn.close()
                except Error as exc:
                    logger.warning(ConnectionCloseError(self._conn, exc))

    def rollback(self) -> None:
        logger.debug("NotImplemented: rollback")

    def _safe_execute(self, f: Callable[[Cursor], Cursor]) -> CursorWrapper:
        with self._lock:
            if not self.open:
                raise DbtRuntimeError("Attempting to execute on a closed connection")
            if self._cursor and self._cursor.open:
                self._cursor.close()
            self._cursor = CursorWrapper(f(self._conn.cursor()))
            return self._cursor

    def __del__(self) -> None:
        if self._cursor and self._cursor.open:
            # This should not happen. The cursor should explicitly be closed.
            logger.debug(CursorClose(self._cursor._cursor))

            self._cursor.close()
            logger.warning("A cursor was closed by destructor.")

        if self.open:
            logger.debug(ConnectionClose(self._conn))

            self._conn.close()
            logger.warning("A connection was closed by destructor.")


def clean_sql(sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1]
    return cleaned


DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")


def extract_dbr_version(version: str) -> tuple[int, int]:
    m = DBR_VERSION_REGEX.search(version)
    if m:
        major = int(m.group(1))
        try:
            minor = int(m.group(2))
        except ValueError:
            minor = sys.maxsize
        return (major, minor)
    else:
        raise DbtRuntimeError("Failed to detect DBR version")


def translate_bindings(bindings: Optional[Sequence[Any]]) -> Optional[Sequence[Any]]:
    if bindings:
        return list(map(lambda x: float(x) if isinstance(x, decimal.Decimal) else x, bindings))
    return None
