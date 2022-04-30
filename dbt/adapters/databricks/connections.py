from contextlib import contextmanager
from distutils.version import LooseVersion
from dataclasses import dataclass
import re
import time
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Optional, Sequence, Tuple

from agate import Table

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.databricks import __version__
from dbt.contracts.connection import Connection, ConnectionState
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.utils import DECIMALS

from dbt.adapters.spark.connections import SparkConnectionManager, _is_retryable_error

from databricks import sql as dbsql
from databricks.sql.client import (
    Connection as DatabricksSQLConnection,
    Cursor as DatabricksSQLCursor,
)
from databricks.sql.exc import DatabaseError, OperationalError

logger = AdapterLogger("Databricks")


@dataclass
class DatabricksCredentials(Credentials):
    host: str
    database: Optional[str]
    http_path: Optional[str] = None
    token: Optional[str] = None
    connect_retries: int = 0
    connect_timeout: int = 10
    session_properties: Optional[Dict[str, Any]] = None
    retry_all: bool = False

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    def __post_init__(self) -> None:
        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

    @property
    def type(self) -> str:
        return "databricks"

    @property
    def unique_field(self) -> str:
        return self.host

    def _connection_keys(self) -> Tuple[str, ...]:
        return "host", "port", "http_path", "schema"


class DatabricksSQLConnectionWrapper(object):
    """Wrap a Databricks SQL connector in a way that no-ops transactions"""

    _conn: DatabricksSQLConnection
    _cursor: Optional[DatabricksSQLCursor]

    def __init__(self, conn: DatabricksSQLConnection):
        self._conn = conn
        self._cursor = None

    def cursor(self) -> "DatabricksSQLConnectionWrapper":
        self._cursor = self._conn.cursor()
        return self

    def cancel(self) -> None:
        if self._cursor:
            try:
                self._cursor.cancel()
            except OperationalError as exc:
                logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self) -> None:
        if self._cursor:
            try:
                self._cursor.close()
            except OperationalError as exc:
                logger.debug("Exception while closing cursor: {}".format(exc))
        self._conn.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Sequence[Tuple]:
        assert self._cursor is not None
        return self._cursor.fetchall()

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        assert self._cursor is not None
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
        assert self._cursor is not None
        return self._cursor.description

    def schemas(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ) -> None:
        assert self._cursor is not None
        self._cursor.schemas(catalog_name=catalog_name, schema_name=schema_name)


class DatabricksConnectionManager(SparkConnectionManager):
    TYPE: ClassVar[str] = "databricks"

    DROP_JAVA_STACKTRACE_REGEX: ClassVar["re.Pattern[str]"] = re.compile(
        r"(?<=Caused by: )(.+?)(?=^\t?at )", re.DOTALL | re.MULTILINE
    )

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        try:
            yield

        except Exception as exc:
            if isinstance(exc, OperationalError) and LooseVersion(dbsql.__version__) < "2.0":
                logger.debug(f"Error while running:\n{sql}")
                logger.debug(exc)
                msg = str(exc)
                m = self.DROP_JAVA_STACKTRACE_REGEX.search(msg)
                if m:
                    msg = f"Query execution failed.\nError message: {m.group().strip()}"
                raise dbt.exceptions.RuntimeException(msg)
            elif isinstance(exc, DatabaseError):
                logger.debug(f"Error while running:\n{sql}")
                logger.debug(exc)
                if hasattr(exc, "context"):
                    if "operation-id" in exc.context:
                        logger.debug(f"operation-id: {exc.context['operation-id']}")
                    if "diagnostic-info" in exc.context:
                        logger.debug(f"diagnostic-info: {exc.context['diagnostic-info']}")
                raise dbt.exceptions.RuntimeException(str(exc))

            logger.debug(f"Error while running:\n{sql}")
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.RuntimeException(msg)
            else:
                raise dbt.exceptions.RuntimeException(str(exc))

    def _execute_cursor(
        self, log_sql: str, f: Callable[[DatabricksSQLConnectionWrapper], None]
    ) -> Table:
        connection = self.get_thread_connection()

        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(log_sql):
            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
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

    def list_schemas(self, database: Optional[str], schema: Optional[str] = None) -> Table:
        database = database if isinstance(database, str) else None
        schema = schema if isinstance(schema, str) else None
        return self._execute_cursor(
            f"GetSchemas(database={database}, schema={schema})",
            lambda cursor: cursor.schemas(catalog_name=database, schema_name=schema),
        )

    @classmethod
    def validate_creds(cls, creds: DatabricksCredentials, required: List[str]) -> None:
        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required to connect to Databricks".format(key)
                )

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds: DatabricksCredentials = connection.credentials
        exc: Optional[Exception] = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.http_path is None:
                    raise dbt.exceptions.DbtProfileError(
                        "`http_path` must set when"
                        " using the dbsql method to connect to Databricks"
                    )
                required_fields = ["host", "http_path", "token"]

                cls.validate_creds(creds, required_fields)

                dbt_databricks_version = __version__.version
                user_agent_entry = f"dbt-databricks/{dbt_databricks_version}"

                conn: DatabricksSQLConnection = dbsql.connect(
                    server_hostname=creds.host,
                    http_path=creds.http_path,
                    access_token=creds.token,
                    session_configuration=creds.session_properties,
                    _user_agent_entry=user_agent_entry,
                )
                handle = DatabricksSQLConnectionWrapper(conn)
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise dbt.exceptions.FailedToConnectException(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectException("failed to connect") from e
        else:
            assert exc is not None
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection
