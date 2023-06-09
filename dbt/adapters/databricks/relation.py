from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type
from dbt.contracts.relation import (
    ComponentName,
)
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.spark.impl import KEY_TABLE_OWNER, KEY_TABLE_STATISTICS
from dbt.dataclass_schema import StrEnum

from dbt.adapters.databricks.utils import remove_undefined
from dbt.utils import filter_null_values, classproperty
from dbt.exceptions import DbtRuntimeError

KEY_TABLE_PROVIDER = "Provider"


@dataclass
class DatabricksQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class DatabricksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


class DatabricksRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materializedview"
    External = "external"
    StreamingTable = "streamingtable"


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(BaseRelation):
    type: Optional[DatabricksRelationType] = None  # type: ignore
    quote_policy: Policy = field(default_factory=lambda: DatabricksQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: DatabricksIncludePolicy())
    quote_character: str = "`"

    metadata: Optional[Dict[str, Any]] = None

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data["path"]:
            data["path"]["database"] = None
        else:
            data["path"]["database"] = remove_undefined(data["path"]["database"])
        return data

    def has_information(self) -> bool:
        return self.metadata is not None

    @property
    def is_materialized_view(self) -> bool:
        return self.type == DatabricksRelationType.MaterializedView

    @property
    def is_streaming_table(self) -> bool:
        return self.type == DatabricksRelationType.StreamingTable

    @property
    def is_delta(self) -> bool:
        assert self.metadata is not None
        return self.metadata.get(KEY_TABLE_PROVIDER) == "delta"

    @property
    def is_hudi(self) -> bool:
        assert self.metadata is not None
        return self.metadata.get(KEY_TABLE_PROVIDER) == "hudi"

    @property
    def owner(self) -> Optional[str]:
        return self.metadata.get(KEY_TABLE_OWNER) if self.metadata is not None else None

    @property
    def stats(self) -> Optional[str]:
        return self.metadata.get(KEY_TABLE_STATISTICS) if self.metadata is not None else None

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        if not search:
            # nothing was passed in
            raise DbtRuntimeError("Tried to match relation, but no search path was passed!")

        match = True

        for k, v in search.items():
            if str(self.path.get_lowered_part(k)).strip(self.quote_character) != v.lower().strip(
                self.quote_character
            ):
                match = False

        return match

    @classproperty
    def get_relation_type(cls) -> Type[DatabricksRelationType]:
        return DatabricksRelationType
