from dataclasses import dataclass
from typing import Any, Dict

from dbt.adapters.spark.relation import SparkRelation
from dbt.exceptions import RuntimeException

from dbt.adapters.databricks.utils import remove_undefined


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(SparkRelation):
    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data["path"]:
            data["path"]["database"] = None
        else:
            data["path"]["database"] = remove_undefined(data["path"]["database"])
        return data

    def __post_init__(self) -> None:
        if self.database != self.schema and self.database:
            raise RuntimeException("Cannot set database in Databricks!")
