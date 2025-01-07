from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Optional, cast

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.databricks.connections import DatabricksConnectionManager
from dbt.adapters.databricks.relation import KEY_TABLE_PROVIDER, is_hive_metastore
from dbt.adapters.databricks.utils import handle_missing_objects
from dbt.adapters.spark.impl import KEY_TABLE_OWNER
from dbt.adapters.sql.impl import SQLAdapter

CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
GET_CATALOG_MACRO_NAME = "get_catalog"
SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"


class MetadataBehavior(ABC):
    @classmethod
    @abstractmethod
    def list_relations_without_caching(
        cls, adapter: SQLAdapter, schema_relation: BaseRelation
    ) -> list[BaseRelation]:
        pass


class DefaultMetadataBehavior(MetadataBehavior):
    @classmethod
    def list_relations_without_caching(
        cls, adapter: SQLAdapter, schema_relation: BaseRelation
    ) -> list[BaseRelation]:
        empty: list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
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
                adapter.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=name,
                    type=adapter.Relation.get_relation_type(kind),
                    metadata=metadata,
                )
            )

        return relations

    @classmethod
    def _get_relations_without_caching(
        cls, adapter: SQLAdapter, relation: BaseRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        if is_hive_metastore(relation.database):
            return cls._get_hive_relations(adapter, relation)
        return cls._get_uc_relations(adapter, relation)

    @staticmethod
    def _get_uc_relations(
        adapter: SQLAdapter, relation: BaseRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}
        results = adapter.execute_macro("get_uc_tables", kwargs=kwargs)
        return [
            (row["table_name"], row["table_type"], row["file_format"], row["table_owner"])
            for row in results
        ]

    @classmethod
    def _get_hive_relations(
        cls, adapter: SQLAdapter, relation: BaseRelation
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        kwargs = {"relation": relation}
        connection_manager = cast(DatabricksConnectionManager, adapter.connections)

        new_rows: list[tuple[str, Optional[str]]]
        if all([relation.database, relation.schema]):
            tables = connection_manager.list_tables(
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
            tables = adapter.execute_macro(SHOW_TABLES_MACRO_NAME, kwargs=kwargs)
            new_rows = [(row["tableName"], None) for row in tables]

        # if there are any table types to be resolved
        if any(not row[1] for row in new_rows):
            with cls._catalog(adapter, relation.database):
                views = adapter.execute_macro(SHOW_VIEWS_MACRO_NAME, kwargs=kwargs)
                view_names = set(views.columns["viewName"].values())  # type: ignore[attr-defined]
                new_rows = [
                    (row[0], str(RelationType.View if row[0] in view_names else RelationType.Table))
                    for row in new_rows
                ]

        return [(row[0], row[1], None, None) for row in new_rows]

    @staticmethod
    @contextmanager
    def _catalog(adapter: SQLAdapter, catalog: Optional[str]) -> Iterator[None]:
        """
        A context manager to make the operation work in the specified catalog,
        and move back to the current catalog after the operation.

        If `catalog` is None, the operation works in the current catalog.
        """
        current_catalog: Optional[str] = None
        try:
            if catalog is not None:
                current_catalog = adapter.execute_macro(CURRENT_CATALOG_MACRO_NAME)[0][0]
                if current_catalog is not None:
                    if current_catalog != catalog:
                        adapter.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=catalog))
                    else:
                        current_catalog = None
            yield
        finally:
            if current_catalog is not None:
                adapter.execute_macro(USE_CATALOG_MACRO_NAME, kwargs=dict(catalog=current_catalog))
