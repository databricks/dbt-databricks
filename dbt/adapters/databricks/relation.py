from dataclasses import dataclass
import re
from typing import Any, Dict, Optional

from dbt.adapters.base.relation import Policy
from dbt.adapters.spark.relation import SparkRelation

from dbt.adapters.databricks.utils import remove_undefined

INFORMATION_LOCATION_REGEX = re.compile(r"^Location: (.*)$", re.MULTILINE)


@dataclass
class DatabricksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(SparkRelation):
    include_policy: DatabricksIncludePolicy = DatabricksIncludePolicy()

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

    @property
    def location(self) -> Optional[str]:
        if self.information is not None and "Type: EXTERNAL" in self.information:
            location_match = re.findall(INFORMATION_LOCATION_REGEX, self.information)
            return location_match[0] if location_match else None
        else:
            return None

    @property
    def location_root(self) -> Optional[str]:
        location = self.location
        if location is not None:
            return location[: location.rfind("/")]
        else:
            return None
