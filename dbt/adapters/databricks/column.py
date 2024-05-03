from dataclasses import dataclass
from typing import ClassVar
from typing import Dict
from typing import Optional

from dbt.adapters.spark.column import SparkColumn


@dataclass
class SimpleColumn:
    name: str
    dtype: str
    table_comment: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class DatabricksColumn(SparkColumn):
    table_comment: Optional[str] = None
    comment: Optional[str] = None

    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "LONG": "BIGINT",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return super(SparkColumn, cls).translate_type(dtype).lower()

    @property
    def data_type(self) -> str:
        return self.translate_type(self.dtype)

    def __repr__(self) -> str:
        return "<DatabricksColumn {} ({})>".format(self.name, self.data_type)

    def to_simple_column(self) -> SimpleColumn:
        return SimpleColumn(
            name=self.name,
            dtype=self.data_type,
            table_comment=self.table_comment,
            comment=self.comment,
        )
