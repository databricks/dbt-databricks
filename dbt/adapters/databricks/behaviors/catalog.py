import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional

from dbt_common.utils import executor
from dbt_common.utils.dict import AttrDict

from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.utils import handle_missing_objects
from dbt.adapters.sql.impl import SQLAdapter

if TYPE_CHECKING:
    from agate import Table


USE_CATALOG_MACRO_NAME = "use_catalog"
CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
SHOW_TABLE_EXTENDED_MACRO_NAME = "show_table_extended"
SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"

INFORMATION_COLUMNS_REGEX = re.compile(r"^ \|-- (.*): (.*) \(nullable = (.*)\b", re.MULTILINE)
INFORMATION_OWNER_REGEX = re.compile(r"^Owner: (.*)$", re.MULTILINE)
INFORMATION_STATISTICS_REGEX = re.compile(r"^Statistics: (.*)$", re.MULTILINE)
INFORMATION_COMMENT_REGEX = re.compile(r"Comment: (.*)\n[A-Z][A-Za-z ]+:", re.DOTALL)


class GetCatalogBehavior(ABC):
    @classmethod
    @abstractmethod
    def get_catalog(
        cls,
        adapter: SQLAdapter,
        relation_configs: Iterable[RelationConfig],
        used_schemas: frozenset[tuple[str, str]],
    ) -> tuple["Table", list[Exception]]:
        pass

    @classmethod
    @abstractmethod
    def get_catalog_by_relations(
        cls,
        adapter: SQLAdapter,
        used_schemas: frozenset[tuple[str, str]],
        relations: set[BaseRelation],
    ) -> tuple["Table", list[Exception]]:
        pass


class DefaultGetCatalogBehavior(GetCatalogBehavior):
    @classmethod
    def get_catalog(
        cls,
        adapter: SQLAdapter,
        relation_configs: Iterable[RelationConfig],
        used_schemas: frozenset[tuple[str, str]],
    ) -> tuple["Table", list[Exception]]:
        relation_map: dict[tuple[str, str], set[str]] = defaultdict(set)
        for relation in relation_configs:
            relation_map[(relation.database or "hive_metastore", relation.schema or "default")].add(
                relation.identifier
            )

        return cls._get_catalog_for_relation_map(adapter, relation_map, used_schemas)

    @classmethod
    def get_catalog_by_relations(
        cls,
        adapter: SQLAdapter,
        used_schemas: frozenset[tuple[str, str]],
        relations: set[BaseRelation],
    ) -> tuple["Table", list[Exception]]:
        relation_map: dict[tuple[str, str], set[str]] = defaultdict(set)
        for relation in relations:
            if relation.identifier:
                relation_map[
                    (relation.database or "hive_metastore", relation.schema or "schema")
                ].add(relation.identifier)

        return cls._get_catalog_for_relation_map(adapter, relation_map, used_schemas)

    @classmethod
    def _get_catalog_for_relation_map(
        cls,
        adapter: SQLAdapter,
        relation_map: dict[tuple[str, str], set[str]],
        used_schemas: frozenset[tuple[str, str]],
    ) -> tuple["Table", list[Exception]]:
        with executor(adapter.config) as tpe:
            futures: list[Future[Table]] = []
            for schema, relations in relation_map.items():
                if schema in used_schemas:
                    identifier = get_identifier_list_string(relations)
                    if identifier:
                        futures.append(
                            tpe.submit_connected(
                                adapter,
                                str(schema),
                                cls._get_schema_for_catalog,
                                adapter,
                                schema[0],
                                schema[1],
                                identifier,
                            )
                        )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    @classmethod
    def _get_schema_for_catalog(
        cls, adapter: SQLAdapter, catalog: str, schema: str, identifier: str
    ) -> "Table":
        # Lazy load to improve startup time
        from agate import Table
        from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER

        columns: list[dict[str, Any]] = []

        if identifier:
            schema_relation = adapter.Relation.create(
                database=catalog or "hive_metastore",
                schema=schema,
                identifier=identifier,
                quote_policy=adapter.config.quoting,
            )
            for relation, information in cls._list_relations_with_information(
                adapter, schema_relation
            ):
                columns.extend(cls._get_columns_for_catalog(relation, information))
        return Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    @classmethod
    def _get_columns_for_catalog(  # type: ignore[override]
        cls, relation: BaseRelation, information: str
    ) -> Iterable[dict[str, Any]]:
        columns = cls.parse_columns_from_information(relation, information)

        for column in columns:
            # convert DatabricksRelation into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            yield as_dict

    @classmethod
    def _list_relations_with_information(
        cls, adapter: SQLAdapter, schema_relation: BaseRelation
    ) -> list[tuple[BaseRelation, str]]:
        results = cls._show_table_extended(adapter, schema_relation)

        relations: list[tuple[BaseRelation, str]] = []
        if results:
            for name, information in results.select(["tableName", "information"]):
                rel_type = RelationType.View if "Type: VIEW" in information else RelationType.Table
                relation = adapter.Relation.create(
                    database=schema_relation.database.lower() if schema_relation.database else None,
                    schema=schema_relation.schema.lower() if schema_relation.schema else None,
                    identifier=name,
                    type=rel_type,
                )
                relations.append((relation, information))

        return relations

    @classmethod
    def _show_table_extended(
        cls, adapter: SQLAdapter, schema_relation: BaseRelation
    ) -> Optional["Table"]:
        kwargs = {"schema_relation": schema_relation}

        def exec() -> AttrDict:
            with cls._catalog(adapter, schema_relation.database):
                return adapter.execute_macro(SHOW_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs)

        return handle_missing_objects(exec, None)

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

    @staticmethod
    def parse_columns_from_information(  # type: ignore[override]
        relation: BaseRelation, information: str
    ) -> list[DatabricksColumn]:
        owner_match = re.findall(INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(INFORMATION_COLUMNS_REGEX, information)
        comment_match = re.findall(INFORMATION_COMMENT_REGEX, information)
        table_comment = comment_match[0] if comment_match else None
        columns = []
        stats_match = re.findall(INFORMATION_STATISTICS_REGEX, information)
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
