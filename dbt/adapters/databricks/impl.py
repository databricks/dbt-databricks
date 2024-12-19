import os
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import metadata
from multiprocessing.context import SpawnContext
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, Union, cast
from uuid import uuid4

from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.config.base import BaseConfig
from dbt_common.exceptions import CompilationError, DbtConfigError, DbtInternalError
from dbt_common.utils import executor
from dbt_common.utils.dict import AttrDict
from packaging import version

from dbt.adapters.base import AdapterConfig, PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed, log_code_execution
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.contracts.connection import AdapterResponse, Connection
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.databricks.behaviors.columns import (
    GetColumnsBehavior,
    GetColumnsByDescribe,
    GetColumnsByInformationSchema,
)
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import (
    DatabricksConnectionManager,
    ExtendedSessionConnectionManager,
)
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.python_models.python_submissions import (
    AllPurposeClusterPythonJobHelper,
    JobClusterPythonJobHelper,
    ServerlessClusterPythonJobHelper,
    WorkflowPythonJobHelper,
)
from dbt.adapters.databricks.relation import (
    KEY_TABLE_PROVIDER,
    DatabricksRelation,
    DatabricksRelationType,
)
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationConfig,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)
from dbt.adapters.databricks.relation_configs.table_format import TableFormat
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.adapters.databricks.utils import get_first_row, handle_missing_objects, redact_credentials
from dbt.adapters.relation_configs import RelationResults
from dbt.adapters.spark.impl import (
    DESCRIBE_TABLE_EXTENDED_MACRO_NAME,
    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
    KEY_TABLE_OWNER,
    KEY_TABLE_STATISTICS,
    LIST_SCHEMAS_MACRO_NAME,
    SparkAdapter,
)

if TYPE_CHECKING:
    from agate import Row, Table

dbt_version = metadata.version("dbt-core")
SUPPORT_MICROBATCH = version.parse(dbt_version) >= version.parse("1.9.0b1")

CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
GET_CATALOG_MACRO_NAME = "get_catalog"
SHOW_TABLE_EXTENDED_MACRO_NAME = "show_table_extended"
SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"


USE_INFO_SCHEMA_FOR_COLUMNS = BehaviorFlag(
    name="use_info_schema_for_columns",
    default=False,
    description=(
        "Use info schema to gather column information to ensure complex types are not truncated."
        "  Incurs some overhead, so disabled by default."
    ),
)  # type: ignore[typeddict-item]

USE_USER_FOLDER_FOR_PYTHON = BehaviorFlag(
    name="use_user_folder_for_python",
    default=False,
    description=(
        "Use the user's home folder for uploading python notebooks."
        "  Shared folder use is deprecated due to governance concerns."
    ),
)  # type: ignore[typeddict-item]


@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    table_format: str = TableFormat.DEFAULT
    location_root: Optional[str] = None
    include_full_name_in_path: bool = False
    partition_by: Optional[Union[list[str], str]] = None
    clustered_by: Optional[Union[list[str], str]] = None
    liquid_clustered_by: Optional[Union[list[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    merge_exclude_columns: Optional[str] = None
    databricks_tags: Optional[dict[str, str]] = None
    tblproperties: Optional[dict[str, str]] = None
    zorder: Optional[Union[list[str], str]] = None
    unique_tmp_table_suffix: bool = False
    skip_non_matched_step: Optional[bool] = None
    skip_matched_step: Optional[bool] = None
    matched_condition: Optional[str] = None
    not_matched_condition: Optional[str] = None
    not_matched_by_source_action: Optional[str] = None
    not_matched_by_source_condition: Optional[str] = None
    target_alias: Optional[str] = None
    source_alias: Optional[str] = None
    merge_with_schema_evolution: Optional[bool] = None


def get_identifier_list_string(table_names: set[str]) -> str:
    """Returns `"|".join(table_names)` by default.

    Returns `"*"` if `DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS` == `"true"`
    and the joined string exceeds 2048 characters

    This is for AWS Glue Catalog users. See issue #325.
    """

    _identifier = "|".join(table_names)
    bypass_2048_char_limit = GlobalState.get_char_limit_bypass()
    if bypass_2048_char_limit == "true":
        _identifier = _identifier if len(_identifier) < 2048 else "*"
    return _identifier


class DatabricksAdapter(SparkAdapter):
    INFORMATION_COMMENT_REGEX = re.compile(r"Comment: (.*)\n[A-Z][A-Za-z ]+:", re.DOTALL)

    Relation = DatabricksRelation
    Column = DatabricksColumn

    if GlobalState.get_use_long_sessions():
        ConnectionManager: type[DatabricksConnectionManager] = ExtendedSessionConnectionManager
    else:
        ConnectionManager = DatabricksConnectionManager

    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    _capabilities = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        }
    )

    get_column_behavior: GetColumnsBehavior

    def __init__(self, config: Any, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)

        # dbt doesn't propogate flags for certain workflows like dbt debug so this requires
        # an additional guard
        self.get_column_behavior = GetColumnsByDescribe()
        try:
            if self.behavior.use_info_schema_for_columns.no_warn:  # type: ignore[attr-defined]
                self.get_column_behavior = GetColumnsByInformationSchema()
        except CompilationError:
            pass

    @property
    def _behavior_flags(self) -> list[BehaviorFlag]:
        return [USE_INFO_SCHEMA_FOR_COLUMNS, USE_USER_FOLDER_FOR_PYTHON]

    @available.parse(lambda *a, **k: 0)
    def update_tblproperties_for_iceberg(
        self, config: BaseConfig, tblproperties: Optional[dict[str, str]] = None
    ) -> dict[str, str]:
        result = tblproperties or config.get("tblproperties", {})
        if config.get("table_format") == TableFormat.ICEBERG:
            if self.compare_dbr_version(14, 3) < 0:
                raise DbtConfigError("Iceberg support requires Databricks Runtime 14.3 or later.")
            if config.get("file_format", "delta") != "delta":
                raise DbtConfigError(
                    "When table_format is 'iceberg', cannot set file_format to other than delta."
                )
            if config.get("materialized") not in ("incremental", "table"):
                raise DbtConfigError(
                    "When table_format is 'iceberg', materialized must be 'incremental' or 'table'."
                )
            result["delta.enableIcebergCompatV2"] = "true"
            result["delta.universalFormat.enabledFormats"] = "iceberg"
        return result

    @available.parse(lambda *a, **k: 0)
    def compute_external_path(
        self, config: BaseConfig, model: BaseConfig, is_incremental: bool = False
    ) -> str:
        location_root = config.get("location_root")
        database = model.get("database", "hive_metastore")
        schema = model.get("schema", "default")
        identifier = model.get("alias")
        if location_root is None:
            raise DbtConfigError("location_root is required for external tables.")
        include_full_name_in_path = config.get("include_full_name_in_path", False)
        if include_full_name_in_path:
            path = os.path.join(location_root, database, schema, identifier)
        else:
            path = os.path.join(location_root, identifier)
        if is_incremental:
            path = path + "_tmp"
        return path

    # override/overload
    def acquire_connection(
        self, name: Optional[str] = None, query_header_context: Any = None
    ) -> Connection:
        return self.connections.set_connection_name(name, query_header_context)

    # override
    @contextmanager
    def connection_named(
        self, name: str, query_header_context: Any = None, should_release_connection: bool = True
    ) -> Iterator[None]:
        try:
            if self.connections.query_header is not None:
                self.connections.query_header.set(name, query_header_context)
            self.acquire_connection(name, query_header_context)
            yield
        finally:
            if should_release_connection:
                self.release_connection()

            if self.connections.query_header is not None:
                self.connections.query_header.reset()

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

    def list_schemas(self, database: Optional[str]) -> list[str]:
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
        limit: Optional[int] = None,
        *,
        staging_table: Optional[BaseRelation] = None,
    ) -> tuple[AdapterResponse, "Table"]:
        try:
            return super().execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)
        finally:
            if staging_table is not None:
                self.drop_relation(staging_table)

    def list_relations_without_caching(  # type: ignore[override]
        self, schema_relation: DatabricksRelation
    ) -> list[DatabricksRelation]:
        empty: list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
        results = handle_missing_objects(
            lambda: self.get_relations_without_caching(schema_relation), empty
        )

        relations = []
        for row in results:
            name, kind, file_format, owner = row
            metadata = None
            if file_format:
                metadata = {KEY_TABLE_OWNER: owner, KEY_TABLE_PROVIDER: file_format}
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=name,
                    type=self.Relation.get_relation_type(kind),
                    metadata=metadata,
                )
            )

        return relations

    def get_relations_without_caching(
        self, relation: DatabricksRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        if relation.is_hive_metastore():
            return self._get_hive_relations(relation)
        return self._get_uc_relations(relation)

    def _get_uc_relations(
        self, relation: DatabricksRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}
        results = self.execute_macro("get_uc_tables", kwargs=kwargs)
        return [
            (row["table_name"], row["table_type"], row["file_format"], row["table_owner"])
            for row in results
        ]

    def _get_hive_relations(
        self, relation: DatabricksRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}

        new_rows: list[tuple[str, Optional[str]]]
        if all([relation.database, relation.schema]):
            tables = self.connections.list_tables(
                database=relation.database,  # type: ignore[arg-type]
                schema=relation.schema,  # type: ignore[arg-type]
            )

            new_rows = []
            for row in tables:
                # list_tables returns TABLE_TYPE as view for both materialized views and for
                # streaming tables.  Set type to "" in this case and it will be resolved below.
                type = row["TABLE_TYPE"].lower() if row["TABLE_TYPE"] else None
                row = (row["TABLE_NAME"], type)
                new_rows.append(row)

        else:
            tables = self.execute_macro(SHOW_TABLES_MACRO_NAME, kwargs=kwargs)
            new_rows = [(row["tableName"], None) for row in tables]

        # if there are any table types to be resolved
        if any(not row[1] for row in new_rows):
            with self._catalog(relation.database):
                views = self.execute_macro(SHOW_VIEWS_MACRO_NAME, kwargs=kwargs)
                view_names = set(views.columns["viewName"].values())  # type: ignore[attr-defined]
                new_rows = [
                    (row[0], str(RelationType.View if row[0] in view_names else RelationType.Table))
                    for row in new_rows
                ]

        return [(row[0], row[1], None, None) for row in new_rows]

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> list[DatabricksColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        _, cursor = self.connections.add_select_query(sql)
        try:
            columns: list[DatabricksColumn] = [
                self.Column.create(
                    column_name, self.connections.data_type_code_to_name(column_type_code)
                )
                # https://peps.python.org/pep-0249/#description
                for column_name, column_type_code, *_ in cursor.description
            ]
        finally:
            cursor.close()
        return columns

    def get_relation(
        self,
        database: Optional[str],
        schema: str,
        identifier: str,
        *,
        needs_information: bool = False,
    ) -> Optional[DatabricksRelation]:
        cached: Optional[DatabricksRelation] = super(SparkAdapter, self).get_relation(
            database=database, schema=schema, identifier=identifier
        )

        if not needs_information:
            return cached

        return self._set_relation_information(cached) if cached else None

    def parse_describe_extended(  # type: ignore[override]
        self, relation: DatabricksRelation, raw_rows: list["Row"]
    ) -> tuple[dict[str, Any], list[DatabricksColumn]]:
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
        return metadata, [
            DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                table_comment=metadata.get("Comment"),
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
                comment=column["comment"],
            )
            for idx, column in enumerate(rows)
        ]

    def get_columns_in_relation(  # type: ignore[override]
        self, relation: DatabricksRelation
    ) -> list[DatabricksColumn]:
        return self.get_column_behavior.get_columns_in_relation(self, relation)

    def _get_updated_relation(
        self, relation: DatabricksRelation
    ) -> tuple[DatabricksRelation, list[DatabricksColumn]]:
        rows = list(
            handle_missing_objects(
                lambda: self.execute_macro(
                    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
                    kwargs={"relation": relation},
                ),
                AttrDict(),
            )
        )
        metadata, columns = self.parse_describe_extended(relation, rows)

        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]

        return (
            self.Relation.create(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier,
                type=relation.type,  # type: ignore
                metadata=metadata,
            ),
            columns,
        )

    def _set_relation_information(self, relation: DatabricksRelation) -> DatabricksRelation:
        """Update the information of the relation, or return it if it already exists."""
        if relation.has_information():
            return relation

        return self._get_updated_relation(relation)[0]

    def parse_columns_from_information(  # type: ignore[override]
        self, relation: DatabricksRelation, information: str
    ) -> list[DatabricksColumn]:
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, information)
        comment_match = re.findall(self.INFORMATION_COMMENT_REGEX, information)
        table_comment = comment_match[0] if comment_match else None
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)

        for match_num, match in enumerate(matches):
            column_name, column_type, _ = match.groups()
            column = DatabricksColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                table_comment=table_comment,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=DatabricksColumn.translate_type(column_type),
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def get_catalog_by_relations(
        self, used_schemas: frozenset[tuple[str, str]], relations: set[BaseRelation]
    ) -> tuple["Table", list[Exception]]:
        relation_map: dict[tuple[str, str], set[str]] = defaultdict(set)
        for relation in relations:
            if relation.identifier:
                relation_map[
                    (relation.database or "hive_metastore", relation.schema or "schema")
                ].add(relation.identifier)

        return self._get_catalog_for_relation_map(relation_map, used_schemas)

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: frozenset[tuple[str, str]],
    ) -> tuple["Table", list[Exception]]:
        relation_map: dict[tuple[str, str], set[str]] = defaultdict(set)
        for relation in relation_configs:
            relation_map[(relation.database or "hive_metastore", relation.schema or "default")].add(
                relation.identifier
            )

        return self._get_catalog_for_relation_map(relation_map, used_schemas)

    def _get_catalog_for_relation_map(
        self,
        relation_map: dict[tuple[str, str], set[str]],
        used_schemas: frozenset[tuple[str, str]],
    ) -> tuple["Table", list[Exception]]:
        with executor(self.config) as tpe:
            futures: list[Future[Table]] = []
            for schema, relations in relation_map.items():
                if schema in used_schemas:
                    identifier = get_identifier_list_string(relations)
                    if identifier:
                        futures.append(
                            tpe.submit_connected(
                                self,
                                str(schema),
                                self._get_schema_for_catalog,
                                schema[0],
                                schema[1],
                                identifier,
                            )
                        )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _list_relations_with_information(
        self, schema_relation: DatabricksRelation
    ) -> list[tuple[DatabricksRelation, str]]:
        results = self._show_table_extended(schema_relation)

        relations: list[tuple[DatabricksRelation, str]] = []
        if results:
            for name, information in results.select(["tableName", "information"]):
                rel_type = RelationType.View if "Type: VIEW" in information else RelationType.Table
                relation = self.Relation.create(
                    database=schema_relation.database.lower() if schema_relation.database else None,
                    schema=schema_relation.schema.lower() if schema_relation.schema else None,
                    identifier=name,
                    type=rel_type,
                )
                relations.append((relation, information))

        return relations

    def _show_table_extended(self, schema_relation: DatabricksRelation) -> Optional["Table"]:
        kwargs = {"schema_relation": schema_relation}

        def exec() -> AttrDict:
            with self._catalog(schema_relation.database):
                return self.execute_macro(SHOW_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs)

        return handle_missing_objects(exec, None)

    def _get_schema_for_catalog(self, catalog: str, schema: str, identifier: str) -> "Table":
        # Lazy load to improve startup time
        from agate import Table
        from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER

        columns: list[dict[str, Any]] = []

        if identifier:
            schema_relation = self.Relation.create(
                database=catalog or "hive_metastore",
                schema=schema,
                identifier=identifier,
                quote_policy=self.config.quoting,
            )
            for relation, information in self._list_relations_with_information(schema_relation):
                columns.extend(self._get_columns_for_catalog(relation, information))
        return Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def _get_columns_for_catalog(  # type: ignore[override]
        self, relation: DatabricksRelation, information: str
    ) -> Iterable[dict[str, Any]]:
        columns = self.parse_columns_from_information(relation, information)

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
    ) -> tuple[Connection, Any]:
        return self.connections.add_query(
            sql, auto_begin, bindings, abridge_sql_log, close_cursor=close_cursor
        )

    def run_sql_for_tests(
        self, sql: str, fetch: str, conn: Connection
    ) -> Optional[Union[Optional[tuple], list[tuple]]]:
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

    @available
    def valid_incremental_strategies(self) -> list[str]:
        valid_strategies = ["append", "merge", "insert_overwrite", "replace_where"]
        if SUPPORT_MICROBATCH:
            valid_strategies.append("microbatch")

        return valid_strategies

    @property
    def python_submission_helpers(self) -> dict[str, type[PythonJobHelper]]:
        return {
            "job_cluster": JobClusterPythonJobHelper,
            "all_purpose_cluster": AllPurposeClusterPythonJobHelper,
            "serverless_cluster": ServerlessClusterPythonJobHelper,
            "workflow_job": WorkflowPythonJobHelper,
        }

    @log_code_execution
    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        parsed_model["config"]["user_folder_for_python"] = parsed_model["config"].get(
            "user_folder_for_python",
            self.behavior.use_user_folder_for_python.setting,  # type: ignore[attr-defined]
        )
        return super().submit_python_job(parsed_model, compiled_code)

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

    @available.parse(lambda *a, **k: {})
    def get_persist_doc_columns(
        self, existing_columns: list[DatabricksColumn], columns: dict[str, Any]
    ) -> dict[str, Any]:
        """Returns a dictionary of columns that have updated comments."""
        return_columns = {}

        # Since existing_columns are gathered after writing the table, we don't need to include any
        # columns from the model that are not in the existing_columns. If we did, it would lead to
        # an error when we tried to alter the table.
        for column in existing_columns:
            name = column.column
            if name in columns:
                config_column = columns[name]
                if isinstance(config_column, dict):
                    comment = columns[name].get("description", "")
                elif hasattr(config_column, "description"):
                    comment = config_column.description
                else:
                    raise DbtInternalError(
                        f"Column {name} in model config is not a dictionary or ColumnInfo object."
                    )
                if comment != (column.comment or ""):
                    return_columns[name] = columns[name]

        return return_columns

    @available.parse(lambda *a, **k: {})
    def get_relation_config(self, relation: DatabricksRelation) -> DatabricksRelationConfigBase:
        if relation.type == DatabricksRelationType.MaterializedView:
            return MaterializedViewAPI.get_from_relation(self, relation)
        elif relation.type == DatabricksRelationType.StreamingTable:
            return StreamingTableAPI.get_from_relation(self, relation)
        elif relation.type == DatabricksRelationType.Table:
            return IncrementalTableAPI.get_from_relation(self, relation)
        else:
            raise NotImplementedError(f"Relation type {relation.type} is not supported.")

    @available.parse(lambda *a, **k: {})
    def get_config_from_model(self, model: RelationConfig) -> DatabricksRelationConfigBase:
        assert model.config, "Config was missing from relation"
        if model.config.materialized == "materialized_view":
            return MaterializedViewAPI.get_from_relation_config(model)
        elif model.config.materialized == "streaming_table":
            return StreamingTableAPI.get_from_relation_config(model)
        elif model.config.materialized == "incremental":
            return IncrementalTableAPI.get_from_relation_config(model)
        else:
            raise NotImplementedError(
                f"Materialization {model.config.materialized} is not supported."
            )

    @available
    def generate_unique_temporary_table_suffix(self, suffix_initial: str = "__dbt_tmp") -> str:
        return f"{suffix_initial}_{str(uuid4())}"


@dataclass(frozen=True)
class RelationAPIBase(ABC, Generic[DatabricksRelationConfig]):
    """Base class for the relation API, so as to provide some encapsulation from the adapter.
    For the most part, these are just namespaces to group related methods together.
    """

    relation_type: ClassVar[str]

    @classmethod
    @abstractmethod
    def config_type(cls) -> type[DatabricksRelationConfig]:
        """Get the config class for delegating calls."""

        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    def get_from_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> DatabricksRelationConfig:
        """Get the relation config from the relation."""

        assert relation.type == cls.relation_type
        results = cls._describe_relation(adapter, relation)
        return cls.config_type().from_results(results)

    @classmethod
    def get_from_relation_config(cls, relation_config: RelationConfig) -> DatabricksRelationConfig:
        """Get the relation config from the model node."""

        return cls.config_type().from_relation_config(relation_config)

    @classmethod
    @abstractmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        """Describe the relation and return the results."""

        raise NotImplementedError("Must be implemented by subclass")


class DeltaLiveTableAPIBase(RelationAPIBase[DatabricksRelationConfig]):
    @classmethod
    def get_from_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> DatabricksRelationConfig:
        """Get the relation config from the relation."""

        relation_config = super().get_from_relation(adapter, relation)

        # Ensure any current refreshes are completed before returning the relation config
        tblproperties = cast(TblPropertiesConfig, relation_config.config["tblproperties"])
        if tblproperties.pipeline_id:
            adapter.connections.api_client.dlt_pipelines.poll_for_completion(
                tblproperties.pipeline_id
            )
        return relation_config


class MaterializedViewAPI(DeltaLiveTableAPIBase[MaterializedViewConfig]):
    relation_type = DatabricksRelationType.MaterializedView

    @classmethod
    def config_type(cls) -> type[MaterializedViewConfig]:
        return MaterializedViewConfig

    @classmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        kwargs = {"table_name": relation}
        results: RelationResults = dict()
        results["describe_extended"] = adapter.execute_macro(
            DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )

        kwargs = {"relation": relation}
        results["information_schema.views"] = cls._get_information_schema_views(adapter, kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results

    @staticmethod
    def _get_information_schema_views(adapter: DatabricksAdapter, kwargs: dict[str, Any]) -> "Row":
        return get_first_row(adapter.execute_macro("get_view_description", kwargs=kwargs))


class StreamingTableAPI(DeltaLiveTableAPIBase[StreamingTableConfig]):
    relation_type = DatabricksRelationType.StreamingTable

    @classmethod
    def config_type(cls) -> type[StreamingTableConfig]:
        return StreamingTableConfig

    @classmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        kwargs = {"table_name": relation}
        results: RelationResults = dict()
        results["describe_extended"] = adapter.execute_macro(
            DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )

        kwargs = {"relation": relation}

        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results


class IncrementalTableAPI(RelationAPIBase[IncrementalTableConfig]):
    relation_type = DatabricksRelationType.Table

    @classmethod
    def config_type(cls) -> type[IncrementalTableConfig]:
        return IncrementalTableConfig

    @classmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        results = {}
        kwargs = {"relation": relation}

        if not relation.is_hive_metastore():
            results["information_schema.tags"] = adapter.execute_macro("fetch_tags", kwargs=kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results
