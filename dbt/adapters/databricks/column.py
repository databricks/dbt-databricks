from dataclasses import dataclass
from typing import Any, ClassVar, Optional

from dbt.adapters.databricks.utils import quote
from dbt.adapters.spark.column import SparkColumn


@dataclass
class DatabricksColumn(SparkColumn):
    table_comment: Optional[str] = None
    comment: Optional[str] = None
    not_null: Optional[bool] = None
    mask: Optional[dict[str, str]] = None
    databricks_tags: Optional[dict[str, str]] = None

    TYPE_LABELS: ClassVar[dict[str, str]] = {
        "LONG": "BIGINT",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return super(SparkColumn, cls).translate_type(dtype).lower()

    @classmethod
    def create(cls, name: str, label_or_dtype: str) -> "DatabricksColumn":
        column_type = cls.translate_type(label_or_dtype)
        return cls(name, column_type)

    @classmethod
    def from_json_metadata(cls, json_metadata: str) -> list["DatabricksColumn"]:
        """
        Parse JSON metadata from DESCRIBE EXTENDED <table> AS JSON into DatabricksColumn objects.

        Args:
            json_metadata: JSON string containing column metadata

        Returns:
            List of DatabricksColumn objects
        """
        import json

        data = json.loads(json_metadata)
        columns = []

        for col_info in data.get("columns", []):
            col_name = col_info.get("name")
            col_type = cls._parse_type_from_json(col_info.get("type"))
            comment = col_info.get("comment")
            columns.append(cls(column=col_name, dtype=col_type, comment=comment))

        return columns

    @classmethod
    def _parse_type_from_json(cls, type_info: Any) -> str:
        """
        Convert type information from JSON format to Databricks DDL.

        This handles complex types from Databricks JSON schema:
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

        Args:
            type_info: Dictionary containing type information from JSON

        Returns:
            String representation of the data type in Databricks DDL format
        """
        type_name = type_info.get("name")

        if type_name == "struct":
            fields = type_info.get("fields", [])
            field_strs = []
            for field in fields:
                field_name = field.get("name")
                field_type = cls._parse_type_from_json(field.get("type"))
                field_strs.append(f"{field_name}:{field_type}")
            return f"struct<{','.join(field_strs)}>"

        elif type_name == "array":
            element_type = cls._parse_type_from_json(type_info.get("element_type"))
            return f"array<{element_type}>"

        elif type_name == "map":
            # Handle map types with element_nullable
            key_type = cls._parse_type_from_json(type_info.get("key_type"))
            value_type = cls._parse_type_from_json(type_info.get("value_type"))
            return f"map<{key_type},{value_type}>"

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
            collation = type_info.get("collation")
            # utf8_binary is the default collation for string types in Databricks
            if collation is None or collation.lower() == "utf8_binary":
                return "string"
            else:
                return f"string COLLATE {collation}"

        elif type_name == "timestamp_ltz":
            return "timestamp"

        elif type_name == "varchar":
            return "string"

        elif type_name == "char":
            return "string"

        else:
            # Handle primitive types and any other types
            return str(type_name)

    @property
    def data_type(self) -> str:
        return self.translate_type(self.dtype)

    def enrich(self, model_column: dict[str, Any], not_null: bool) -> "DatabricksColumn":
        """Create a copy that incorporates model column metadata, including constraints."""

        data_type = model_column.get("data_type") or self.dtype
        name = self.get_name(model_column)
        enriched_column = DatabricksColumn.create(name, data_type)
        if model_column.get("description"):
            enriched_column.comment = model_column["description"]
        if model_column.get("column_mask"):
            enriched_column.mask = model_column["column_mask"]
        if model_column.get("databricks_tags"):
            enriched_column.databricks_tags = model_column["databricks_tags"]

        enriched_column.not_null = not_null
        return enriched_column

    def render_for_create(self) -> str:
        """Renders the column for building a create statement."""
        column_str = f"{self.name} {self.dtype}"
        if self.not_null:
            column_str += " NOT NULL"
        if self.comment:
            comment = self.comment.replace("'", "\\'")
            column_str += f" COMMENT '{comment}'"
        if self.mask:
            column_str += f" MASK {self.mask['function']}"
            if "using_columns" in self.mask:
                column_str += f" USING COLUMNS ({self.mask['using_columns']})"
        return column_str

    def __repr__(self) -> str:
        return f"<DatabricksColumn {self.name} ({self.data_type})>"

    @staticmethod
    def get_name(column: dict[str, Any]) -> str:
        name = column["name"]
        return quote(name) if column.get("quote", False) else name

    @staticmethod
    def format_remove_column_list(columns: list["DatabricksColumn"]) -> str:
        return ", ".join([quote(c.name) for c in columns])

    @staticmethod
    def format_add_column_list(columns: list["DatabricksColumn"]) -> str:
        return ", ".join([f"{quote(c.name)} {c.data_type}" for c in columns])
