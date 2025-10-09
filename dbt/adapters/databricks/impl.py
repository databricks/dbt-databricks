import posixpath
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import metadata
from multiprocessing.context import SpawnContext
from typing import TYPE_CHECKING, Any, ClassVar, Generic, NamedTuple, Optional, Union, cast
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
from dbt.adapters.catalogs import CatalogRelation
from dbt.adapters.contracts.connection import AdapterResponse, Connection
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.databricks import constants, constraints, parse_model
from dbt.adapters.databricks.behaviors.columns import (
    GetColumnsBehavior,
    GetColumnsByDescribe,
    GetColumnsByInformationSchema,
)
from dbt.adapters.databricks.catalogs import (
    DatabricksCatalogRelation,
    HiveMetastoreCatalogIntegration,
    UnityCatalogIntegration,
)
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.handle import SqlUtils
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
from dbt.adapters.databricks.relation_configs.column_tags import (
    ColumnTagsConfig,
    ColumnTagsProcessor,
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
from dbt.adapters.databricks.relation_configs.view import ViewConfig
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

USE_MATERIALIZATION_V2 = BehaviorFlag(
    name="use_materialization_v2",
    default=False,
    description=(
        "Use revamped materializations based on separating create and insert."
        "  This allows more performant column comments, as well as new column features."
    ),
)  # type: ignore[typeddict-item]


class DatabricksRelationInfo(NamedTuple):
    table_name: str
    table_type: str
    file_format: Optional[str]
    table_owner: Optional[str]
    databricks_table_type: Optional[str]


@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    table_format: str = TableFormat.DEFAULT
    location_root: Optional[str] = None
    include_full_name_in_path: bool = False
    partition_by: Optional[Union[list[str], str]] = None
    clustered_by: Optional[Union[list[str], str]] = None
    liquid_clustered_by: Optional[Union[list[str], str]] = None
    auto_liquid_cluster: Optional[bool] = None
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
    use_safer_relation_operations: Optional[bool] = None
    incremental_apply_config_changes: Optional[bool] = None
    view_update_via_alter: Optional[bool] = None


def get_identifier_list_string(table_names: set[str]) -> str:
    """Returns `"|".join(table_names)` by default.

    Returns `"*"` if `DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS` == `"true"`
    and the joined string exceeds 2048 characters

    This is for AWS Glue Catalog users. See issue #325.
    """

    _identifier = "|".join(table_names)
    bypass_2048_char_limit = GlobalState.get_char_limit_bypass()
    if bypass_2048_char_limit:
        _identifier = _identifier if len(_identifier) < 2048 else "*"
    return _identifier


class DatabricksAdapter(SparkAdapter):
    INFORMATION_COMMENT_REGEX = re.compile(r"Comment: (.*)\n[A-Z][A-Za-z ]+:", re.DOTALL)

    Relation = DatabricksRelation
    Column = DatabricksColumn

    ConnectionManager = DatabricksConnectionManager

    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    _capabilities = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        }
    )

    CATALOG_INTEGRATIONS = [
        HiveMetastoreCatalogIntegration,
        UnityCatalogIntegration,
    ]
    CONSTRAINT_SUPPORT = constraints.CONSTRAINT_SUPPORT

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

        self.add_catalog_integration(constants.DEFAULT_UNITY_CATALOG)
        self.add_catalog_integration(constants.DEFAULT_HIVE_METASTORE_CATALOG)

    @property
    def _behavior_flags(self) -> list[BehaviorFlag]:
        return [USE_INFO_SCHEMA_FOR_COLUMNS, USE_USER_FOLDER_FOR_PYTHON, USE_MATERIALIZATION_V2]

    @available.parse(lambda *a, **k: 0)
    def update_tblproperties_for_iceberg(
        self, config: BaseConfig, tblproperties: Optional[dict[str, str]] = None
    ) -> dict[str, str]:
        result = tblproperties or config.get("tblproperties", {})

        catalog_relation: DatabricksCatalogRelation = self.build_catalog_relation(config.model)  # type:ignore
        if catalog_relation.table_format == constants.ICEBERG_TABLE_FORMAT:
            if self.compare_dbr_version(14, 3) < 0:
                raise DbtConfigError("Iceberg support requires Databricks Runtime 14.3 or later.")
            if config.get("materialized") not in ("incremental", "table", "snapshot"):
                raise DbtConfigError(
                    "When table_format is 'iceberg', materialized must be 'incremental'"
                    ", 'table', or 'snapshot'."
                )
            result.update(catalog_relation.iceberg_table_properties)

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
            path = posixpath.join(location_root, database, schema, identifier)
        else:
            path = posixpath.join(location_root, identifier)
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
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})
        return [row[0] for row in results]

    def check_schema_exists(self, database: Optional[str], schema: str) -> bool:
        """Check if a schema exists."""
        return schema.lower() in set(
            s.lower() for s in self.connections.list_schemas(database or "hive_metastore", schema)
        )

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
        empty: list[DatabricksRelationInfo] = []
        results = handle_missing_objects(
            lambda: self.get_relations_without_caching(schema_relation), empty
        )

        relations = []
        for row in results:
            name, kind, file_format, owner, table_type = row
            metadata = None
            if file_format:
                metadata = {KEY_TABLE_OWNER: owner, KEY_TABLE_PROVIDER: file_format}

            if table_type:
                databricks_table_type = DatabricksRelation.get_databricks_table_type(table_type)
            else:
                databricks_table_type = None

            relations.append(
                DatabricksRelation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=name,
                    type=DatabricksRelation.get_relation_type(kind),
                    databricks_table_type=databricks_table_type,
                    metadata=metadata,
                    is_delta=file_format == "delta",
                )
            )

        return relations

    def get_relations_without_caching(
        self, relation: DatabricksRelation
    ) -> list[DatabricksRelationInfo]:
        if relation.is_hive_metastore():
            return self._get_hive_relations(relation)
        return self._get_uc_relations(relation)

    def _get_uc_relations(self, relation: DatabricksRelation) -> list[DatabricksRelationInfo]:
        kwargs = {"relation": relation}
        results = self.execute_macro("get_uc_tables", kwargs=kwargs)
        return [
            DatabricksRelationInfo(
                row["table_name"],
                row["table_type"],
                row["file_format"],
                row["table_owner"],
                row["databricks_table_type"],
            )
            for row in results
        ]

    def _get_hive_relations(self, relation: DatabricksRelation) -> list[DatabricksRelationInfo]:
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
                rel_type = row["TABLE_TYPE"].lower() if row["TABLE_TYPE"] else None
                row = (row["TABLE_NAME"], rel_type)
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

        return [
            DatabricksRelationInfo(
                row[0],
                row[1],  # type: ignore[arg-type]
                None,
                None,
                None,
            )
            for row in new_rows
        ]

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
        # Use legacy macros for hive metastore or DBR versions older than 16.2
        use_legacy_logic = (
            relation.is_hive_metastore()
            or self.compare_dbr_version(16, 2) < 0
            or relation.type == DatabricksRelationType.MaterializedView
            # TODO: Replace with self.compare_dbr_version(17, 1) < 0 when 17.1 is current version
            #       for SQL warehouses
            or relation.type == DatabricksRelationType.StreamingTable
        )
        return self.get_column_behavior.get_columns_in_relation(self, relation, use_legacy_logic)

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
            DatabricksRelation.create(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier,
                type=relation.type,  # type: ignore
                databricks_table_type=relation.databricks_table_type,
                metadata=metadata,
                is_delta=metadata.get(KEY_TABLE_PROVIDER) == "delta",
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
                rel_type = self._get_relation_type(information)
                relation = self.Relation.create(
                    database=schema_relation.database.lower() if schema_relation.database else None,
                    schema=schema_relation.schema.lower() if schema_relation.schema else None,
                    identifier=name,
                    type=rel_type,  # type: ignore
                    is_delta="Provider: delta" in information,
                )
                relations.append((relation, information))

        return relations

    def _get_relation_type(self, information: str) -> str:
        if "Type: VIEW" in information:
            return RelationType.View
        if "TYPE: MATERIALIZED_VIEW" in information:
            return DatabricksRelationType.MaterializedView
        if "TYPE: STREAMING_TABLE" in information:
            return DatabricksRelationType.StreamingTable
        return DatabricksRelationType.Table

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
            schema_relation = DatabricksRelation.create(
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
        handle = conn.handle
        try:
            cursor = handle.execute(sql)
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
            if cursor:
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
                    comment = columns[name].get("description")
                elif hasattr(config_column, "description"):
                    comment = config_column.description
                else:
                    raise DbtInternalError(
                        f"Column {name} in model config is not a dictionary or ColumnInfo object."
                    )
                if comment != (column.comment or ""):
                    return_columns[name] = columns[name]

        return return_columns

    @available
    @staticmethod
    def generate_unique_temporary_table_suffix(suffix_initial: str = "__dbt_tmp") -> str:
        return f"{suffix_initial}_{re.sub(r'[^A-Za-z0-9]+', '_', str(uuid4()))}"

    @available
    @staticmethod
    def parse_columns_and_constraints(
        existing_columns: list[DatabricksColumn],
        model_columns: dict[str, dict[str, Any]],
        model_constraints: list[dict[str, Any]],
    ) -> tuple[list[DatabricksColumn], list[constraints.TypedConstraint]]:
        """Returns a list of columns that have been updated with features for table create."""
        enriched_columns = []
        not_null_set, parsed_constraints = constraints.parse_constraints(
            list(model_columns.values()), model_constraints
        )

        for column in existing_columns:
            if column.name in model_columns:
                column_info = model_columns[column.name]
                enriched_column = column.enrich(column_info, column.name in not_null_set)
                enriched_columns.append(enriched_column)
            else:
                if column.name in not_null_set:
                    column.not_null = True
                enriched_columns.append(column)

        return enriched_columns, parsed_constraints

    @available.parse(lambda *a, **k: {})
    def get_relation_config(self, relation: DatabricksRelation) -> DatabricksRelationConfigBase:
        if relation.type == DatabricksRelationType.MaterializedView:
            return MaterializedViewAPI.get_from_relation(self, relation)
        elif relation.type == DatabricksRelationType.StreamingTable:
            return StreamingTableAPI.get_from_relation(self, relation)
        elif relation.type == DatabricksRelationType.Table:
            return IncrementalTableAPI.get_from_relation(self, relation)
        elif relation.type == DatabricksRelationType.View:
            return ViewAPI.get_from_relation(self, relation)
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
        elif model.config.materialized == "view":
            return ViewAPI.get_from_relation_config(model)
        else:
            raise NotImplementedError(
                f"Materialization {model.config.materialized} is not supported."
            )

    @available
    def is_cluster(self) -> bool:
        """Check if the current connection is a cluster."""
        return self.connections.is_cluster()

    @available.parse(lambda *a, **k: {})
    def clean_sql(self, sql: str) -> str:
        return SqlUtils.clean_sql(sql)

    @available
    def build_catalog_relation(self, model: RelationConfig) -> Optional[CatalogRelation]:
        """
        Builds a relation for a given configuration.

        This method uses the provided configuration to determine the appropriate catalog
        integration and config parser for building the relation. It defaults to the built-in Delta
        catalog if none is provided in the configuration for backward compatibility.

        Args:
            model (RelationConfig): `config.model` (not `model`) from the jinja context

        Returns:
            Any: The relation object generated through the catalog integration and parser
        """
        if catalog := parse_model.catalog_name(model):
            catalog_integration = self.get_catalog_integration(catalog)
            return catalog_integration.build_relation(model)
        return None

    @available
    def get_column_tags_from_model(self, model: RelationConfig) -> Optional[ColumnTagsConfig]:
        return ColumnTagsProcessor.from_relation_config(model)


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
        results["information_schema.views"] = get_first_row(
            adapter.execute_macro("get_view_description", kwargs=kwargs)
        )
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results


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
            results["information_schema.column_tags"] = adapter.execute_macro(
                "fetch_column_tags", kwargs=kwargs
            )
            results["non_null_constraint_columns"] = adapter.execute_macro(
                "fetch_non_null_constraint_columns", kwargs=kwargs
            )
            results["primary_key_constraints"] = adapter.execute_macro(
                "fetch_primary_key_constraints", kwargs=kwargs
            )
            results["foreign_key_constraints"] = adapter.execute_macro(
                "fetch_foreign_key_constraints", kwargs=kwargs
            )
            results["column_masks"] = adapter.execute_macro("fetch_column_masks", kwargs=kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)

        kwargs = {"table_name": relation}
        results["describe_extended"] = adapter.execute_macro(
            DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )
        return results


class ViewAPI(RelationAPIBase[ViewConfig]):
    relation_type = DatabricksRelationType.View

    @classmethod
    def config_type(cls) -> type[ViewConfig]:
        return ViewConfig

    @classmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        results = {}
        kwargs = {"relation": relation}

        results["information_schema.views"] = get_first_row(
            adapter.execute_macro("get_view_description", kwargs=kwargs)
        )
        results["information_schema.tags"] = adapter.execute_macro("fetch_tags", kwargs=kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)

        kwargs = {"table_name": relation}
        results["describe_extended"] = adapter.execute_macro(
            DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )
        return results
