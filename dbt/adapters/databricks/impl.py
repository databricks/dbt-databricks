from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
import os
import re
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union, cast
import uuid

from agate import Row, Table
from requests.exceptions import HTTPError

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed, log_code_execution
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.spark.impl import (
    SparkAdapter,
    KEY_TABLE_OWNER,
    KEY_TABLE_STATISTICS,
    LIST_RELATIONS_MACRO_NAME,
    LIST_SCHEMAS_MACRO_NAME,
)
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import RelationType
import dbt.exceptions
from dbt.events import AdapterLogger
from dbt.utils import executor

from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.api_client import Api12Client
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import (
    DatabricksConnectionManager,
    DatabricksCredentials,
    DBT_DATABRICKS_INVOCATION_ENV,
)
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.utils import undefined_proof


logger = AdapterLogger("Databricks")

CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"


@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    tblproperties: Optional[Dict[str, str]] = None


@undefined_proof
class DatabricksAdapter(SparkAdapter):

    Relation = DatabricksRelation
    Column = DatabricksColumn

    ConnectionManager = DatabricksConnectionManager
    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    def list_schemas(self, database: Optional[str]) -> List[str]:
        """
        Get a list of existing schemas in database.

        If `database` is `None`, fallback to executing `show databases` because
        `list_schemas` tries to collect schemas from all catalogs when `database` is `None`.
        """
        if database is not None:
            results = self.connections.list_schemas(database=database)
        else:
            results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})
        return [row[0] for row in results]

    def check_schema_exists(self, database: Optional[str], schema: str) -> bool:
        """Check if a schema exists."""
        return schema.lower() in set(s.lower() for s in self.list_schemas(database=database))

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        *,
        staging_table: Optional[BaseRelation] = None,
    ) -> Tuple[AdapterResponse, Table]:
        try:
            return super().execute(sql=sql, auto_begin=auto_begin, fetch=fetch)
        finally:
            if staging_table is not None:
                self.drop_relation(staging_table)

    def list_relations_without_caching(
        self, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        kwargs = {"schema_relation": schema_relation}
        try:
            # The catalog for `show table extended` needs to match the current catalog.
            with self._catalog(schema_relation.database):
                results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "show table extended ...", '
                    f"got {len(row)} values, expected 4"
                )
            _schema, name, _, information = row
            rel_type = RelationType.View if "Type: VIEW" in information else RelationType.Table
            is_delta = "Provider: delta" in information
            is_hudi = "Provider: hudi" in information
            relation = self.Relation.create(
                database=schema_relation.database,
                schema=_schema,
                identifier=name,
                type=rel_type,
                information=information,
                is_delta=is_delta,
                is_hudi=is_hudi,
            )
            relations.append(relation)

        return relations

    def parse_describe_extended(
        self, relation: DatabricksRelation, raw_rows: List[Row]
    ) -> List[DatabricksColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)
        return [
            DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                column=column["col_name"],
                column_index=(idx + 1),
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    def parse_columns_from_information(
        self, relation: DatabricksRelation
    ) -> List[DatabricksColumn]:
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, relation.information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, relation.information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, relation.information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=(match_num + 1),
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def get_catalog(self, manifest: Manifest) -> Tuple[Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(manifest)

        with executor(self.config) as tpe:
            futures: List[Future[Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self, schema, self._get_one_catalog, info, [schema], manifest
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_columns_for_catalog(self, relation: DatabricksRelation) -> Iterable[Dict[str, Any]]:
        columns = self.parse_columns_from_information(relation)

        for column in columns:
            # convert DatabricksRelation into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            yield as_dict

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        *,
        close_cursor: bool = False,
    ) -> Tuple[Connection, Any]:
        return self.connections.add_query(
            sql, auto_begin, bindings, abridge_sql_log, close_cursor=close_cursor
        )

    def run_sql_for_tests(
        self, sql: str, fetch: str, conn: Connection
    ) -> Optional[Union[Optional[Tuple], List[Tuple]]]:
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return None
        except BaseException as e:
            print(sql)
            print(e)
            raise
        finally:
            cursor.close()
            conn.transaction_open = False

    def valid_incremental_strategies(self) -> List[str]:
        return ["append", "merge", "insert_overwrite"]

    @available.parse_none
    @log_code_execution
    def submit_python_job(
        self, parsed_model: dict, compiled_code: str, timeout: Optional[int] = None
    ) -> AdapterResponse:
        # TODO limit this function to run only when doing the materialization of python nodes

        credentials: DatabricksCredentials = self.config.credentials

        if not timeout:
            timeout = 60 * 60 * 24
        if timeout <= 0:
            raise ValueError("Timeout must larger than 0")

        cluster_id = credentials.cluster_id
        if not cluster_id:
            raise ValueError("Python model doesn't support SQL Warehouses")

        command_name = f"dbt-databricks_{version}"

        invocation_env = os.environ.get(DBT_DATABRICKS_INVOCATION_ENV)
        if invocation_env is not None and len(invocation_env) > 0:
            self.ConnectionManager.validate_invocation_env(invocation_env)
            command_name = f"{command_name}-{invocation_env}"

        command_name += "-" + str(uuid.uuid1())

        api_client = Api12Client(
            host=credentials.host, token=cast(str, credentials.token), command_name=command_name
        )

        try:
            # Create an execution context
            context_id = api_client.Context.create(cluster_id)

            try:
                # Run a command
                command_id = api_client.Command.execute(
                    cluster_id=cluster_id,
                    context_id=context_id,
                    command=compiled_code,
                )

                # poll until job finish
                status: str
                start = time.time()
                exceeded_timeout = False
                terminal_states = ("Cancelled", "Error", "Finished")
                while True:
                    response = api_client.Command.status(
                        cluster_id=cluster_id, context_id=context_id, command_id=command_id
                    )
                    status = response["status"]
                    if status in terminal_states:
                        break
                    if time.time() - start > timeout:
                        exceeded_timeout = True
                        break
                    time.sleep(3)
                if exceeded_timeout:
                    raise dbt.exceptions.RuntimeException("python model run timed out")
                if status != "Finished":
                    raise dbt.exceptions.RuntimeException(
                        "python model run ended in state "
                        f"{status} with state_message\n{response['results']['data']}"
                    )
                if response["results"]["resultType"] == "error":
                    raise dbt.exceptions.RuntimeException(
                        f"Python model failed with traceback as:\n{response['results']['cause']}"
                    )

                return self.connections.get_response(None)

            finally:
                # Delete the execution context
                api_client.Context.destroy(cluster_id=cluster_id, context_id=context_id)
        except HTTPError as e:
            raise dbt.exceptions.RuntimeException(str(e)) from e
        finally:
            api_client.close()

    @contextmanager
    def _catalog(self, catalog: Optional[str]) -> Iterator[None]:
        """
        A context manager to make the operation work in the specified catalog,
        and move back to the current catalog after the operation.

        If `catalog` is None, the operation works in the current catalog.
        """
        current_catalog: Optional[str] = None
        try:
            if catalog is not None:
                current_catalog = self.execute_macro(CURRENT_CATALOG_MACRO_NAME)[0][0]
                if current_catalog is not None:
                    if current_catalog != catalog:
                        self.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=catalog))
                    else:
                        current_catalog = None
            yield
        finally:
            if current_catalog is not None:
                self.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=current_catalog))
