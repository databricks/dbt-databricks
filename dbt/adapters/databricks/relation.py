from dataclasses import dataclass
from typing import Any, Dict

from dbt.adapters.base.relation import Policy
from dbt.adapters.spark.relation import SparkRelation

from dbt.adapters.databricks.utils import remove_undefined


@dataclass
class DatabricksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(SparkRelation):
    include_policy: DatabricksIncludePolicy = DatabricksIncludePolicy()  # type: ignore[assignment]

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data["path"]:
            data["path"]["database"] = None
        else:
            data["path"]["database"] = remove_undefined(data["path"]["database"])
        return data

    def __post_init__(self) -> None:
        return

    def render(self) -> str:
        return super(SparkRelation, self).render()
