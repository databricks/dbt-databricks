import os
import re
from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from typing import ClassVar
from typing import Dict
from typing import FrozenSet
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union

from agate import Row
from agate import Table
from agate import Text
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.capability import Capability
from dbt.adapters.capability import CapabilityDict
from dbt.adapters.capability import CapabilitySupport
from dbt.adapters.capability import Support
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.connections import ExtendedSessionConnectionManager
from dbt.adapters.databricks.connections import USE_LONG_SESSIONS
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.python_submissions import (
    DbtDatabricksAllPurposeClusterPythonJobHelper,
)
from dbt.adapters.databricks.python_submissions import (
    DbtDatabricksJobClusterPythonJobHelper,
)
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation import extract_identifiers
from dbt.adapters.databricks.relation import is_hive_metastore
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase
from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.materialized_view import (
    MaterializedViewConfig,
)
from dbt.adapters.databricks.relation_configs.streaming_table import (
    StreamingTableConfig,
)
from dbt.adapters.databricks.utils import get_first_row
from dbt.adapters.databricks.utils import redact_credentials
from dbt.adapters.databricks.utils import undefined_proof
from dbt.adapters.relation_configs import RelationResults
from dbt.adapters.spark.impl import DESCRIBE_TABLE_EXTENDED_MACRO_NAME
from dbt.adapters.spark.impl import GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME
from dbt.adapters.spark.impl import KEY_TABLE_OWNER
from dbt.adapters.spark.impl import KEY_TABLE_STATISTICS
from dbt.adapters.spark.impl import LIST_SCHEMAS_MACRO_NAME
from dbt.adapters.spark.impl import SparkAdapter
from dbt.adapters.spark.impl import TABLE_OR_VIEW_NOT_FOUND_MESSAGES
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import executor

CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
GET_CATALOG_MACRO_NAME = "get_catalog"
SHOW_TABLE_EXTENDED_MACRO_NAME = "show_table_extended"
SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"
GET_COLUMNS_COMMENTS_MACRO_NAME = "get_columns_comments"


@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    liquid_clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    databricks_tags: Optional[Dict[str, str]] = None
    tblproperties: Optional[Dict[str, str]] = None
    zorder: Optional[Union[List[str], str]] = None


def check_not_found_error(errmsg: str) -> bool:
    new_error = "[SCHEMA_NOT_FOUND]" in errmsg
    old_error = re.match(r".*(Database).*(not found).*", errmsg, re.DOTALL)
    return new_error or old_error is not None


def get_identifier_list_string(table_names: Set[str]) -> str:
    """Returns `"|".join(table_names)` by default.

    Returns `"*"` if `DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS` == `"true"`
    and the joined string exceeds 2048 characters

    This is for AWS Glue Catalog users. See issue #325.
    """

    _identifier = "|".join(table_names)
    bypass_2048_char_limit = os.environ.get("DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS", "false")
    if bypass_2048_char_limit == "true":
        _identifier = _identifier if len(_identifier) < 2048 else "*"
    return _identifier


@undefined_proof
class DatabricksAdapter(SparkAdapter):
    INFORMATION_COMMENT_REGEX = re.compile(r"Comment: (.*)\n[A-Z][A-Za-z ]+:", re.DOTALL)

    Relation = DatabricksRelation
    Column = DatabricksColumn

    if USE_LONG_SESSIONS:
        ConnectionManager: Type[DatabricksConnectionManager] = ExtendedSessionConnectionManager
    else:
        ConnectionManager = DatabricksConnectionManager

    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    _capabilities = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.NotImplemented),
        }
    )

    # override/overload
    def acquire_connection(
        self, name: Optional[str] = None, query_header_context: Any = None
    ) -> Connection:
        return self.connections.set_connection_name(name, query_header_context)

    # override
    @contextmanager
    def connection_named(self, name: str, query_header_context: Any = None) -> Iterator[None]:
        try:
            if self.connections.query_header is not None:
                self.connections.query_header.set(name, query_header_context)
            self.acquire_connection(name, query_header_context)
            yield
        finally:
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
        limit: Optional[int] = None,
        *,
        staging_table: Optional[BaseRelation] = None,
    ) -> Tuple[AdapterResponse, Table]:
        try:
            return super().execute(sql=sql, auto_begin=auto_begin, fetch=fetch, limit=limit)
        finally:
            if staging_table is not None:
                self.drop_relation(staging_table)

    def list_relations_without_caching(  # type: ignore[override]
        self, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        try:
            results = self.get_relations_without_caching(schema_relation)
        except DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if check_not_found_error(errmsg):
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                raise e

        return [
            self.Relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=name,
                type=self.Relation.get_relation_type(kind),
                comment=comment if comment != "" else None,
                file_format=file_format,
                owner=owner,
            )
            for name, comment, kind, file_format, owner in results.select(
                ["name", "comment", "kind", "file_format", "owner"]
            )
        ]

    def _list_relations_with_information(
        self, schema_relation: DatabricksRelation
    ) -> List[Tuple[DatabricksRelation, str]]:
        results: List[Row]
        kwargs = {"schema_relation": schema_relation}
        try:
            # The catalog for `show table extended` needs to match the current catalog.
            with self._catalog(schema_relation.database):
                results = list(self.execute_macro(SHOW_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs))
        except DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if check_not_found_error(errmsg):
                results = []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation.without_identifier()}: {e.msg}")
                raise e

        relations: List[Tuple[DatabricksRelation, str]] = []
        for row in results:
            if len(row) != 4:
                raise DbtRuntimeError(
                    f'Invalid value from "show table extended ...", '
                    f"got {len(row)} values, expected 4"
                )
            _schema, name, _, information = row
            rel_type = RelationType.View if "Type: VIEW" in information else RelationType.Table
            relation = self.Relation.create(
                database=schema_relation.database,
                # Use `_schema` retrieved from the cluster to avoid mismatched case
                # between the profile and the cluster.
                schema=_schema,
                identifier=name,
                type=rel_type,
            )
            relations.append((relation, information))

        return relations

    def get_relations_without_caching(self, relation: DatabricksRelation) -> Table:
        if relation.is_hive_metastore():
            return self._get_hive_relations(relation)
        return self._get_uc_relations(relation)

    def _get_uc_relations(self, relation: DatabricksRelation) -> Table:
        kwargs = {"relation": relation}
        tables = self.execute_macro("get_uc_tables", kwargs=kwargs)
        return Table(
            tables,
            column_names=["name", "comment", "kind", "file_format", "owner"],
            column_types=[Text(), Text(), Text(), Text(), Text()],
        )

    def _get_hive_relations(self, relation: DatabricksRelation) -> Table:
        kwargs = {"relation": relation}

        new_rows: List[Tuple[str, Optional[str]]]
        if all([relation.database, relation.schema]):
            tables = self.connections.list_tables(
                database=relation.database, schema=relation.schema  # type: ignore[arg-type]
            )

            new_rows = []
            for row in tables:
                # list_tables returns TABLE_TYPE as view for both materialized views and for
                # streaming tables.  Set type to "" in this case and it will be resolved below.
                type = row["TABLE_TYPE"].lower() if row["TABLE_TYPE"].lower() != "view" else None
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

        return Table(
            [(row[0], None, row[1], None, None) for row in new_rows],
            column_names=["name", "comment", "kind", "file_format", "owner"],
            column_types=[Text(), Text(), Text(), Text(), Text()],
        )

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
        self, relation: DatabricksRelation, raw_rows: List[Row]
    ) -> Tuple[Dict[str, Any], List[DatabricksColumn]]:
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
    ) -> List[DatabricksColumn]:
        try:
            rows: List[Row] = list(
                self.execute_macro(GET_COLUMNS_COMMENTS_MACRO_NAME, kwargs={"relation": relation})
            )
            columns = []
            for row in rows:
                if row["col_name"].startswith("#"):
                    break
                columns.append(
                    DatabricksColumn(
                        column=row["col_name"], dtype=row["data_type"], comment=row["comment"]
                    )
                )
        except DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                columns = []
            else:
                raise e

        return columns

    def _get_updated_relation(
        self, relation: DatabricksRelation
    ) -> Tuple[DatabricksRelation, List[DatabricksColumn]]:
        try:
            rows: List[Row] = list(
                self.execute_macro(
                    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
                    kwargs={"relation": relation},
                )
            )
            metadata, columns = self.parse_describe_extended(relation, rows)
        except DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                metadata = None
                columns = []
            else:
                raise e

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
    ) -> List[DatabricksColumn]:
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        comment_match = re.findall(self.INFORMATION_COMMENT_REGEX, information)
        table_comment = comment_match[0] if comment_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = DatabricksColumn.convert_table_stats(raw_table_stats)

        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
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

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple[Table, List[Exception]]:  # type: ignore
        schema_map = self._get_catalog_schemas(relation_configs)

        with executor(self.config) as tpe:
            futures: List[Future[Table]] = []
            for info, schemas in schema_map.items():
                if is_hive_metastore(info.database):
                    for schema in schemas:
                        futures.append(
                            tpe.submit_connected(
                                self,
                                "hive_metastore",
                                self._get_hive_catalog,
                                schema,
                                "*",
                            )
                        )
                else:
                    name = ".".join([str(info.database), "information_schema"])
                    fut = tpe.submit_connected(
                        self, name, self._get_one_unity_catalog, info, used_schemas
                    )
                    futures.append(fut)
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_unity_catalog(
        self, info: InformationSchema, schemas: FrozenSet[Tuple[str, str]]
    ) -> Table:
        kwargs = {
            "information_schema": info,
            "schemas": schemas,
        }
        table = self.execute_macro(GET_CATALOG_MACRO_NAME, kwargs=kwargs)

        results = self._catalog_filter_table(table, schemas)
        return results

    def get_catalog_by_relations(
        self, used_schemas: FrozenSet[Tuple[str, str]], relations: Set[BaseRelation]
    ) -> Tuple[Table, List[Exception]]:
        with executor(self.config) as tpe:
            relations_by_catalog = self._get_catalog_relations_by_info_schema(relations)
            futures: List[Future[Table]] = []

            for info_schema, catalog_relations in relations_by_catalog.items():
                if is_hive_metastore(info_schema.database):
                    schema_map = defaultdict(list)
                    for relation in catalog_relations:
                        schema_map[relation.schema].append(relation)

                    for schema, schema_relations in schema_map.items():
                        table_names = extract_identifiers(schema_relations)
                        futures.append(
                            tpe.submit_connected(
                                self,
                                "hive_metastore",
                                self._get_hive_catalog,
                                schema,
                                get_identifier_list_string(table_names),
                            )
                        )
                else:
                    name = ".".join([str(info_schema.database), "information_schema"])
                    fut = tpe.submit_connected(
                        self,
                        name,
                        self._get_one_catalog_by_relations,
                        info_schema,
                        catalog_relations,
                        used_schemas,
                    )
                    futures.append(fut)

            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_hive_catalog(self, schema: str, identifier: str) -> Table:
        columns: List[Dict[str, Any]] = []

        if identifier:
            schema_relation = self.Relation.create(
                database="hive_metastore",
                schema=schema,
                identifier=identifier,
                quote_policy=self.config.quoting,
            )
            for relation, information in self._list_relations_with_information(schema_relation):
                columns.extend(self._get_columns_for_catalog(relation, information))
        return Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def _get_columns_for_catalog(  # type: ignore[override]
        self, relation: DatabricksRelation, information: str
    ) -> Iterable[Dict[str, Any]]:
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
        return ["append", "merge", "insert_overwrite", "replace_where"]

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

    @available.parse(lambda *a, **k: {})
    def get_persist_doc_columns(
        self, existing_columns: List[DatabricksColumn], columns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Returns a dictionary of columns that have updated comments."""
        return_columns = {}

        # Since existing_columns are gathered after writing the table, we don't need to include any
        # columns from the model that are not in the existing_columns. If we did, it would lead to
        # an error when we tried to alter the table.
        for column in existing_columns:
            name = column.column
            if (
                name in columns
                and "description" in columns[name]
                and columns[name]["description"] != (column.comment or "")
            ):
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


@dataclass(frozen=True)
class RelationAPIBase(ABC, Generic[DatabricksRelationConfig]):
    """Base class for the relation API, so as to provide some encapsulation from the adapter.
    For the most part, these are just namespaces to group related methods together.
    """

    relation_type: ClassVar[DatabricksRelationType]

    @classmethod
    @abstractmethod
    def config_type(cls) -> Type[DatabricksRelationConfig]:
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


class MaterializedViewAPI(RelationAPIBase[MaterializedViewConfig]):
    relation_type = DatabricksRelationType.MaterializedView

    @classmethod
    def config_type(cls) -> Type[MaterializedViewConfig]:
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
    def _get_information_schema_views(adapter: DatabricksAdapter, kwargs: Dict[str, Any]) -> Row:
        row = get_first_row(adapter.execute_macro("get_view_description", kwargs=kwargs))
        if "view_definition" in row.keys() and row["view_definition"] is not None:
            return row
        return get_first_row(adapter.execute_macro("get_view_description_alt", kwargs=kwargs))


class StreamingTableAPI(RelationAPIBase[StreamingTableConfig]):
    relation_type = DatabricksRelationType.StreamingTable

    @classmethod
    def config_type(cls) -> Type[StreamingTableConfig]:
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
    def config_type(cls) -> Type[IncrementalTableConfig]:
        return IncrementalTableConfig

    @classmethod
    def _describe_relation(
        cls, adapter: DatabricksAdapter, relation: DatabricksRelation
    ) -> RelationResults:
        results = {}
        kwargs = {"relation": relation}

        if not relation.is_hive_metastore():
            results["information_schema.tags"] = adapter.execute_macro("fetch_tags", kwargs=kwargs)
        return results
