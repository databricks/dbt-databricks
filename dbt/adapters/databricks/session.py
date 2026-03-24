"""
Session mode support for dbt-databricks.

This module provides SparkSession-based execution for running dbt on Databricks job clusters
without requiring the DBSQL connector. It enables complete dbt pipelines (SQL + Python models)
to execute entirely within a single SparkSession.

Key components:
- SessionCursorWrapper: Adapts DataFrame results to the cursor interface expected by dbt
- DatabricksSessionHandle: Wraps SparkSession to provide the handle interface
"""

import sys
from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.handle import SqlUtils
from dbt.adapters.databricks.logging import logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession
    from pyspark.sql.types import StructField


class SessionCursorWrapper:
    """
    Wraps SparkSession DataFrame results to provide a cursor-like interface.

    This adapter allows dbt to use SparkSession.sql() results in the same way
    it uses DBSQL cursor results, maintaining compatibility with the existing
    connection management code.
    """

    def __init__(self, spark: "SparkSession"):
        self._spark = spark
        self._df: Optional["DataFrame"] = None
        self._rows: Optional[list["Row"]] = None
        self._query_id: str = "session-query"
        self.open = True

    def execute(
        self, sql: str, bindings: Optional[Sequence[Any]] = None
    ) -> "SessionCursorWrapper":
        """Execute a SQL statement and store the resulting DataFrame."""
        cleaned_sql = SqlUtils.clean_sql(sql)

        # Handle bindings by simple string substitution if provided
        if bindings:
            translated = SqlUtils.translate_bindings(bindings)
            if translated:
                cleaned_sql = cleaned_sql % tuple(translated)

        logger.debug(f"Session mode executing SQL: {cleaned_sql[:200]}...")
        self._df = self._spark.sql(cleaned_sql)
        self._rows = None  # Reset cached rows
        return self

    def fetchall(self) -> Sequence[tuple]:
        """Fetch all rows from the result set."""
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        return [tuple(row) for row in (self._rows or [])]

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row from the result set."""
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        if self._rows:
            return tuple(self._rows.pop(0))
        return None

    def fetchmany(self, size: int) -> Sequence[tuple]:
        """Fetch the next `size` rows from the result set."""
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        if not self._rows:
            return []
        result = [tuple(row) for row in self._rows[:size]]
        self._rows = self._rows[size:]
        return result

    @property
    def description(self) -> Optional[list[tuple]]:
        """Return column descriptions in DB-API format."""
        if self._df is None:
            return None
        return [self._field_to_description(f) for f in self._df.schema.fields]

    @staticmethod
    def _field_to_description(field: "StructField") -> tuple:
        """Convert a StructField to a DB-API description tuple."""
        # DB-API description: (name, type_code, display_size, internal_size,
        #                      precision, scale, null_ok)
        return (
            field.name,
            field.dataType.simpleString(),
            None,
            None,
            None,
            None,
            field.nullable,
        )

    def get_response(self) -> AdapterResponse:
        """Return an adapter response for the executed query."""
        return AdapterResponse(_message="OK", query_id=self._query_id)

    def cancel(self) -> None:
        """Cancel the current operation (no-op for session mode)."""
        logger.debug("SessionCursorWrapper.cancel() called (no-op)")
        self.open = False

    def close(self) -> None:
        """Close the cursor."""
        logger.debug("SessionCursorWrapper.close() called")
        self.open = False
        self._df = None
        self._rows = None

    def __str__(self) -> str:
        return f"SessionCursor(query-id={self._query_id})"

    def __enter__(self) -> "SessionCursorWrapper":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return exc_val is None


class DatabricksSessionHandle:
    """
    Handle for a Databricks SparkSession.

    Provides the same interface as DatabricksHandle but uses the active SparkSession
    instead of the DBSQL connector. This enables dbt to run on job clusters without
    requiring external API connections.
    """

    def __init__(self, spark: "SparkSession"):
        self._spark = spark
        self.open = True
        self._cursor: Optional[SessionCursorWrapper] = None
        self._dbr_version: Optional[tuple[int, int]] = None

    @staticmethod
    def create(
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        session_properties: Optional[dict[str, Any]] = None,
    ) -> "DatabricksSessionHandle":
        """
        Create a DatabricksSessionHandle using the active SparkSession.

        Args:
            catalog: Optional catalog to set as current
            schema: Optional schema to set as current database
            session_properties: Optional session configuration properties

        Returns:
            A new DatabricksSessionHandle instance
        """
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        # Set catalog if provided
        if catalog:
            try:
                spark.catalog.setCurrentCatalog(catalog)
                logger.debug(f"Set current catalog to: {catalog}")
            except Exception as e:
                logger.warning(f"Failed to set catalog '{catalog}': {e}")
                # Fall back to USE CATALOG for older Spark versions
                spark.sql(f"USE CATALOG {catalog}")

        # Set schema/database if provided
        if schema:
            spark.catalog.setCurrentDatabase(schema)
            logger.debug(f"Set current database to: {schema}")

        # Apply session properties
        if session_properties:
            for key, value in session_properties.items():
                spark.conf.set(key, str(value))
                logger.debug(f"Set session property {key}={value}")

        handle = DatabricksSessionHandle(spark)
        logger.debug(f"Created session handle: {handle}")
        return handle

    @property
    def dbr_version(self) -> tuple[int, int]:
        """Get the DBR version of the current cluster."""
        if self._dbr_version is None:
            try:
                version_str = self._spark.conf.get(
                    "spark.databricks.clusterUsageTags.sparkVersion", ""
                )
                if version_str:
                    self._dbr_version = SqlUtils.extract_dbr_version(version_str)
                else:
                    # If we can't get the version, assume latest
                    logger.warning("Could not determine DBR version, assuming latest")
                    self._dbr_version = (sys.maxsize, sys.maxsize)
            except Exception as e:
                logger.warning(f"Failed to get DBR version: {e}, assuming latest")
                self._dbr_version = (sys.maxsize, sys.maxsize)
        return self._dbr_version

    @property
    def session_id(self) -> str:
        """Get a unique identifier for this session."""
        try:
            # Try to get the Spark application ID as a session identifier
            return self._spark.sparkContext.applicationId or "session-unknown"
        except Exception:
            return "session-unknown"

    def execute(
        self, sql: str, bindings: Optional[Sequence[Any]] = None
    ) -> SessionCursorWrapper:
        """Execute a SQL statement and return a cursor wrapper."""
        if not self.open:
            raise DbtRuntimeError("Attempting to execute on a closed session handle")

        if self._cursor:
            self._cursor.close()

        self._cursor = SessionCursorWrapper(self._spark)
        return self._cursor.execute(sql, bindings)

    def list_schemas(
        self, database: str, schema: Optional[str] = None
    ) -> SessionCursorWrapper:
        """List schemas in the given database/catalog."""
        if schema:
            sql = f"SHOW SCHEMAS IN {database} LIKE '{schema}'"
        else:
            sql = f"SHOW SCHEMAS IN {database}"
        return self.execute(sql)

    def list_tables(self, database: str, schema: str) -> SessionCursorWrapper:
        """List tables in the given database and schema."""
        sql = f"SHOW TABLES IN {database}.{schema}"
        return self.execute(sql)

    def cancel(self) -> None:
        """Cancel any in-progress operations."""
        logger.debug("DatabricksSessionHandle.cancel() called")
        if self._cursor:
            self._cursor.cancel()
        self.open = False

    def close(self) -> None:
        """Close the session handle."""
        logger.debug("DatabricksSessionHandle.close() called")
        if self._cursor:
            self._cursor.close()
        self.open = False
        # Note: We don't stop the SparkSession as it may be shared

    def rollback(self) -> None:
        """Required for interface compatibility, but not implemented."""
        logger.debug("NotImplemented: rollback (session mode)")

    def __del__(self) -> None:
        if self._cursor:
            self._cursor.close()
        self.close()

    def __str__(self) -> str:
        return f"SessionHandle(session-id={self.session_id})"
