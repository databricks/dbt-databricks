from abc import ABC, abstractmethod
from typing import Any

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
        json_metadata = cls._get_columns_with_comments(
            adapter, relation, "get_columns_comments_as_json"
        )[0]["json_metadata"]
        return cls._parse_columns_from_json(json_metadata)

    @classmethod
    def _parse_columns_from_json(cls, json_metadata: str) -> list[DatabricksColumn]:
        import json

        data = json.loads(json_metadata)
        columns = []

        for col_info in data.get("columns", []):
            col_name = col_info.get("name")
            col_type = cls._parse_type(col_info.get("type"))
            comment = col_info.get("comment")
            columns.append(DatabricksColumn(column=col_name, dtype=col_type, comment=comment))

        return columns

    @classmethod
    def _parse_type(cls, type_info: Any) -> str:
        """
        Convert type information from JSON format to Databricks DDL
        https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table#json-formatted-output

        Complex types with properties other than type name in JSON schema:
          - struct: nested types handled
          - array: nested types handled
          - map: nested types handled
          - decimal: precision, scale handled
          - string: collation handled
          - varchar: Handled just in case, but the JSON should never contain a varchar type as
                     these are just STRING types under the hood in Databricks.
          - char: Handled just in case, but the JSON should never contain a char type as these are
                  just STRING types under the hood in Databricks.

        Complex types can have other properties in the JSON schema such as nullable, defaults, etc.
        but those are ignored as they are not part of data type DDL
        """

        type_name = type_info.get("name")
        if type_name == "struct":
            fields = type_info.get("fields", [])
            field_strs = []
            for field in fields:
                field_name = field.get("name")
                field_type = cls._parse_type(field.get("type"))
                field_strs.append(f"{field_name}: {field_type}")
            return f"struct<{', '.join(field_strs)}>"

        elif type_name == "array":
            element_type = cls._parse_type(type_info.get("element_type"))
            return f"array<{element_type}>"

        elif type_name == "map":
            # Handle map types with element_nullable
            key_type = cls._parse_type(type_info.get("key_type"))
            value_type = cls._parse_type(type_info.get("value_type"))
            return f"map<{key_type}, {value_type}>"

        elif type_name == "decimal":
            # Handle decimal types with precision and scale
            precision = type_info.get("precision")
            scale = type_info.get("scale")
            if precision is not None and scale is not None:
                return f"decimal({precision}, {scale})"
            elif precision is not None:
                return f"decimal({precision})"
            else:
                return "decimal"

        elif type_name == "string":
            # Handle string types with collation
            collation = type_info.get("collation")
            if collation is not None:
                return f"string COLLATE {collation}"
            else:
                return "string"

        elif type_name == "varchar":
            return "string"

        elif type_name == "char":
            return "string"

        else:
            # Handle primitive types and any other types
            return str(type_name)


class GetColumnsByInformationSchema(GetColumnsByDescribe):
    @classmethod
    def get_columns_in_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation
    ) -> list[DatabricksColumn]:
        if (
            relation.is_hive_metastore()
            or relation.type == DatabricksRelation.View
            or not relation.is_delta
        ):
            return super().get_columns_in_relation(adapter, relation)

        rows = cls._get_columns_with_comments(
            adapter, relation, "get_columns_comments_via_information_schema"
        )
        return cls._parse_info_columns(rows)

    @classmethod
    def _parse_info_columns(cls, rows: list[AttrDict]) -> list[DatabricksColumn]:
        return [DatabricksColumn(column=row[0], dtype=row[1], comment=row[2]) for row in rows]
