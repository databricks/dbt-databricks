from dataclasses import dataclass

from dbt.exceptions import RuntimeException

from dbt.adapters.spark.relation import SparkRelation


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(SparkRelation):
    def __post_init__(self) -> None:
        if self.database != self.schema and self.database:
            raise RuntimeException("Cannot set database in Databricks!")
