from dataclasses import dataclass
from typing import Any, ClassVar, Optional

from dbt.adapters.databricks.utils import quote
from dbt.adapters.spark.column import SparkColumn


@dataclass
class DatabricksColumn(SparkColumn):
    table_comment: Optional[str] = None
    comment: Optional[str] = None
    not_null: Optional[bool] = None

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
