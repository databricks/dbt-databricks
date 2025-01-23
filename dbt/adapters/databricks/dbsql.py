import decimal
import re
import sys
import warnings
from collections.abc import Sequence
from threading import RLock
from typing import TYPE_CHECKING, Any, Optional

from dbt_common.exceptions import DbtRuntimeError

from databricks.sql import Error
from databricks.sql.client import Connection as DatabricksSQLConnection
from databricks.sql.client import Cursor as DatabricksSQLCursor
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.databricks.events.connection_events import (
    ConnectionCancel,
    ConnectionCancelError,
    ConnectionClose,
    ConnectionCloseError,
)
from dbt.adapters.databricks.events.cursor_events import (
    CursorCancel,
    CursorClose,
    CursorCloseError,
)
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    from agate import Table


DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")


class DatabricksHandle:
    _dbr_version: tuple[int, int]

    def __init__(
        self,
        conn: DatabricksSQLConnection,
        is_cluster: bool,
    ):
        self._conn = conn
        self.open = True
        self._cursor: Optional[DatabricksSQLCursor] = None
        self._is_cluster = is_cluster
        self.lock = RLock()

    @property
    def dbr_version(self) -> tuple[int, int]:
        with self.lock:
            self._enforce_open()
            if not hasattr(self, "_dbr_version"):
                if self._is_cluster:
                    self._cursor = self._reset_cursor()
                    self._cursor.execute("SET spark.databricks.clusterUsageTags.sparkVersion")
                    results = self._cursor.fetchone()
                    if results:
                        dbr_version: str = results[1]
                    self._cursor.close()
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

    @property
    def description(self) -> Optional[list[tuple]]:
        return self._cursor.description if self._cursor else None

    def _enforce_open(self) -> None:
        if not self.open:
            raise DbtRuntimeError("Connection has already been closed")

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        with self.lock:
            self._enforce_open()
            self._cursor = self._reset_cursor()
            if sql.strip().endswith(";"):
                sql = sql.strip()[:-1]
            if bindings:
                bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, bindings)

    def get_response(self) -> AdapterResponse:
        query_id = self._cursor.query_id if self._cursor and self._cursor.query_id else "N/A"
        return AdapterResponse(_message="OK", query_id=query_id)

    def list_schemas(self, database: str, schema: Optional[str] = None) -> "Table":
        with self.lock:
            self._enforce_open()
            self._cursor = self._reset_cursor()
            self._cursor.schemas(catalog_name=database, schema_name=schema)

    def list_tables(self, database: str, schema: str) -> "Table":
        with self.lock:
            self._enforce_open()
            self._cursor = self._reset_cursor()
            self._cursor.schemas(
                catalog_name=database,
                schema_name=schema,
            )

    def fetchall(self) -> Sequence[tuple]:
        assert self._cursor, "No cursor to fetch from"
        return self._cursor.fetchall()

    def fetchone(self) -> Optional[tuple]:
        assert self._cursor, "No cursor to fetch from"
        return self._cursor.fetchone()

    def fetchmany(self, size: int) -> Sequence[tuple]:
        assert self._cursor, "No cursor to fetch from"
        return self._cursor.fetchmany(size)

    def cancel(self) -> None:
        with self.lock:
            if self.open:
                self.open = False
                logger.debug(ConnectionCancel(self._conn))

                if self._cursor and self._cursor.active_op_handle:
                    logger.debug(CursorCancel(self._cursor))
                    try:
                        self._cursor.cancel()
                    except Error as exc:
                        logger.warning(ConnectionCancelError(self._conn, exc))
                    self._cursor = None

    def close(self) -> None:
        with self.lock:
            if self.open:
                self.open = False
                logger.debug(ConnectionClose(self._conn))

                try:
                    self._conn.close()
                except Error as exc:
                    logger.warning(ConnectionCloseError(self._conn, exc))

    def close_cursor(self) -> None:
        if self._cursor and self._cursor.open:
            logger.debug(CursorClose(self._cursor))

            try:
                self._cursor.close()
            except Error as exc:
                logger.warning(CursorCloseError(self._cursor, exc))

    def rollback(self) -> None:
        logger.debug("NotImplemented: rollback")

    def _fix_binding(cls, value: Any) -> Any:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, decimal.Decimal):
            return float(value)
        else:
            return value

    def _reset_cursor(self) -> DatabricksSQLCursor:
        if self._cursor and self._cursor.open:
            self._cursor.close()
        return self._conn.cursor()

    def __del__(self) -> None:
        if self._cursor and self._cursor.open:
            # This should not happen. The cursor should explicitly be closed.
            logger.debug(CursorClose(self._cursor))

            self._cursor.close()
            with warnings.catch_warnings():
                warnings.simplefilter("always")
                warnings.warn("The cursor was closed by destructor.")
