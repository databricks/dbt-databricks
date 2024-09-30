from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Protocol

from dbt.adapters.databricks import utils
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.utils import handle_missing_objects
from dbt_common.utils.dict import AttrDict
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.spark.impl import KEY_TABLE_OWNER
from dbt.adapters.databricks.relation import KEY_TABLE_PROVIDER
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.spark.impl import GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agate import Row

SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"


class MetadataAdapterProtocol(Protocol):
    connections: DatabricksConnectionManager

    def execute_macro(
        self,
        macro_name: str,
        macro_resolver: Optional[MacroResolverProtocol] = None,
        project: Optional[str] = None,
        context_override: Optional[Dict[str, Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        needs_conn: bool = False,
    ) -> AttrDict:
        pass

    @contextmanager
    def _catalog(self, catalog: Optional[str]) -> Iterator[None]:
        pass

    def parse_describe_extended(
        self, relation: DatabricksRelation, raw_rows: List["Row"]
    ) -> Tuple[Dict[str, Any], List[DatabricksColumn]]:
        pass


class GatherMetadataBehavior(ABC):
    @classmethod
    @abstractmethod
    def list_relations_without_caching(
        cls, adapter: MetadataAdapterProtocol, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        pass

    @classmethod
    @abstractmethod
    def get_relation(
        cls,
        adapter: MetadataAdapterProtocol,
        cached: DatabricksRelation,
        needs_information: bool = False,
    ) -> DatabricksRelation:
        pass


class DefaultGatherBehavior(GatherMetadataBehavior):
    @classmethod
    def list_relations_without_caching(
        cls, adapter: MetadataAdapterProtocol, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        empty: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
        results = handle_missing_objects(
            lambda: cls._get_relations_without_caching(adapter, schema_relation), empty
        )

        relations = []
        for row in results:
            name, kind, file_format, owner = row
            metadata = None
            if file_format:
                metadata = {KEY_TABLE_OWNER: owner, KEY_TABLE_PROVIDER: file_format}
            relations.append(
                DatabricksRelation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=name,
                    type=DatabricksRelation.get_relation_type(kind),
                    metadata=metadata,
                )
            )

        return relations

    @classmethod
    def get_relation(
        cls,
        adapter: MetadataAdapterProtocol,
        cached: DatabricksRelation,
        needs_information: bool = False,
    ) -> DatabricksRelation:
        if not needs_information or cached.has_information():
            return cached

        rows = list(
            handle_missing_objects(
                lambda: adapter.execute_macro(
                    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
                    kwargs={"relation": cached},
                ),
                AttrDict(),
            )
        )
        metadata, _ = adapter.parse_describe_extended(cached, rows)

        return DatabricksRelation.create(
            database=cached.database,
            schema=cached.schema,
            identifier=cached.identifier,
            type=cached.type,  # type: ignore
            metadata=metadata,
        )

    @classmethod
    def _get_relations_without_caching(
        cls, adapter: MetadataAdapterProtocol, relation: DatabricksRelation
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        if relation.is_hive_metastore():
            return cls._get_hive_relations(adapter, relation)
        return cls._get_uc_relations(adapter, relation)

    @classmethod
    def _get_uc_relations(
        cls, adapter: MetadataAdapterProtocol, relation: DatabricksRelation
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}
        results = adapter.execute_macro("get_uc_tables", kwargs=kwargs)
        return [
            (row["table_name"], row["table_type"], row["file_format"], row["table_owner"])
            for row in results
        ]

    @classmethod
    def _get_hive_relations(
        cls, adapter: MetadataAdapterProtocol, relation: DatabricksRelation
    ) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}

        new_rows: List[Tuple[str, Optional[str]]]
        if all([relation.database, relation.schema]):
            tables = adapter.connections.list_tables(
                database=relation.database, schema=relation.schema  # type: ignore[arg-type]
            )

            new_rows = []
            for row in tables:
                # list_tables returns TABLE_TYPE as view for both materialized views and for
                # streaming tables.  Set type to "" in this case and it will be resolved below.
                type = row["TABLE_TYPE"].lower() if row["TABLE_TYPE"] else None
                row = (row["TABLE_NAME"], type)
                new_rows.append(row)

        else:
            tables = adapter.execute_macro(SHOW_TABLES_MACRO_NAME, kwargs=kwargs)
            new_rows = [(row["tableName"], None) for row in tables]

        # if there are any table types to be resolved
        if any(not row[1] for row in new_rows):
            with adapter._catalog(relation.database):
                views = adapter.execute_macro(SHOW_VIEWS_MACRO_NAME, kwargs=kwargs)
                view_names = set(views.columns["viewName"].values())  # type: ignore[attr-defined]
                new_rows = [
                    (row[0], str(RelationType.View if row[0] in view_names else RelationType.Table))
                    for row in new_rows
                ]

        return [(row[0], row[1], None, None) for row in new_rows]


class DelayMetadataGatherBehavior(GatherMetadataBehavior):
    @classmethod
    def list_relations_without_caching(
        cls, adapter: MetadataAdapterProtocol, schema_relation: DatabricksRelation
    ) -> List[DatabricksRelation]:
        results: List[str] = handle_missing_objects(
            lambda: cls._get_object_names(adapter, schema_relation), []
        )

        return [
            DatabricksRelation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=result,
            )
            for result in results
        ]

    @classmethod
    def _get_object_names(
        cls, adapter: MetadataAdapterProtocol, relation: DatabricksRelation
    ) -> List[str]:
        table = adapter.connections.list_tables(
            database=relation.database or "hive_metastore", schema=relation.schema or "default"
        )
        return [row["TABLE_NAME"] for row in table]

    @classmethod
    def get_relation(
        cls,
        adapter: MetadataAdapterProtocol,
        cached: DatabricksRelation,
        needs_information: bool = True,
    ) -> DatabricksRelation:
        raw_rows = list(
            handle_missing_objects(
                lambda: adapter.execute_macro(
                    GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME,
                    kwargs={"relation": cached},
                ),
                AttrDict(),
            )
        )

        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = utils.find_table_information_separator(dict_rows)
        # Remove rows that start with a hash, they are comments
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        return DatabricksRelation.create(
            database=cached.database,
            schema=cached.schema,
            identifier=cached.identifier,
            type=DatabricksRelation.translate_type(metadata["Type"]),  # type: ignore
            metadata=metadata,
        )
