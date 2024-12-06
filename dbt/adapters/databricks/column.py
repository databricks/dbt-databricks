from dataclasses import dataclass
from typing import Any, ClassVar, Optional

from dbt.adapters.databricks.utils import quote
from dbt.adapters.spark.column import SparkColumn


@dataclass
class DatabricksColumn(SparkColumn):
    table_comment: Optional[str] = None
    comment: Optional[str] = None

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
