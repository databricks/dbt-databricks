from abc import ABC, abstractmethod

from dbt_common.utils.dict import AttrDict

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.utils import handle_missing_objects
from dbt.adapters.sql import SQLAdapter


class GetColumnsBehavior(ABC):
    @classmethod
    @abstractmethod
    def get_columns_in_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation
    ) -> list[DatabricksColumn]:
        pass

    @staticmethod
    def _get_columns_with_comments(
        adapter: SQLAdapter, relation: DatabricksRelation, macro_name: str
    ) -> list[AttrDict]:
        return list(
            handle_missing_objects(
                lambda: adapter.execute_macro(macro_name, kwargs={"relation": relation}),
                AttrDict(),
            )
        )


class GetColumnsByDescribe(GetColumnsBehavior):
    @classmethod
    def get_columns_in_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation
    ) -> list[DatabricksColumn]:
        rows = cls._get_columns_with_comments(adapter, relation, "get_columns_comments")
        return cls._parse_columns(rows)

    @classmethod
    def _parse_columns(cls, rows: list[AttrDict]) -> list[DatabricksColumn]:
        columns = []

        for row in rows:
            if row["col_name"].startswith("#"):
                break
            columns.append(
                DatabricksColumn(
                    column=row["col_name"], dtype=row["data_type"], comment=row["comment"]
                )
            )

        return columns


class GetColumnsByInformationSchema(GetColumnsByDescribe):
    @classmethod
    def get_columns_in_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation
    ) -> list[DatabricksColumn]:
        if relation.is_hive_metastore() or relation.type == DatabricksRelation.View:
            return super().get_columns_in_relation(adapter, relation)

        rows = cls._get_columns_with_comments(
            adapter, relation, "get_columns_comments_via_information_schema"
        )
        return cls._parse_columns(rows)

    @classmethod
    def _parse_columns(cls, rows: list[AttrDict]) -> list[DatabricksColumn]:
        return [DatabricksColumn(column=row[0], dtype=row[1], comment=row[2]) for row in rows]
