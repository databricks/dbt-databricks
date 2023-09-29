from dataclasses import dataclass

from dbt.adapters.base.relation import Policy
from dbt.dataclass_schema import StrEnum

class DatabricksRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materializedview"
    External = "external"
    StreamingTable = "streamingtable"

@dataclass
class DatabricksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class DatabricksQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True

