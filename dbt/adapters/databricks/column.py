from dataclasses import dataclass
from threading import RLock
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.databricks.logging import logger
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


class ColumnCache:
    def __init__(self) -> None:
        self._columns: Dict[
            Tuple[Optional[str], Optional[str], Optional[str]], List[DatabricksColumn]
        ] = {}
        self.lock = RLock()

    def update(self, relation: BaseRelation, columns: List[DatabricksColumn]) -> None:
        with self.lock:
            key = (relation.database, relation.schema, relation.identifier)
            self._columns[key] = columns

    def get(self, relation: BaseRelation) -> List[DatabricksColumn]:
        with self.lock:
            key = (relation.database, relation.schema, relation.identifier)
            return self._columns.get(key, [])
