from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union, cast

from agate import Row, Table

from dbt.adapters.base import AdapterConfig, PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed
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

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.python_submissions import (
    DbtDatabricksAllPurposeClusterPythonJobHelper,
    DbtDatabricksJobClusterPythonJobHelper,
)
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.utils import redact_credentials, undefined_proof


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

    @available.parse(lambda *a, **k: 0)
    def compare_dbr_version(self, major: int, minor: int) -> int:
        """
        Returns the comparison result between the version of the cluster and the specified version.

        - positive number if the cluster version is greater than the specified version.
        - 0 if the versions are the same
        - negative number if the cluster version is less than the specified version.

        Always returns positive number if trying to connect to SQL Warehouse.
        """
        return self.connections.compare_dbr_version(major, minor)

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

    def list_relations_without_caching(  # type: ignore[override]
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

    def parse_describe_extended(  # type: ignore[override]
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

    def parse_columns_from_information(  # type: ignore[override]
        self, relation: DatabricksRelation
    ) -> List[DatabricksColumn]:
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, cast(str, relation.information))
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, cast(str, relation.information))
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, cast(str, relation.information))
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

    def _get_columns_for_catalog(  # type: ignore[override]
        self, relation: DatabricksRelation
    ) -> Iterable[Dict[str, Any]]:
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
            print(redact_credentials(sql))
            print(e)
            raise
        finally:
            cursor.close()
            conn.transaction_open = False

    def valid_incremental_strategies(self) -> List[str]:
        return ["append", "merge", "insert_overwrite"]

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "job_cluster": DbtDatabricksJobClusterPythonJobHelper,
            "all_purpose_cluster": DbtDatabricksAllPurposeClusterPythonJobHelper,
        }

    @available
    def redact_credentials(self, sql: str) -> str:
        return redact_credentials(sql)

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
