import threading
from collections import namedtuple
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.base.relation import Policy
from dbt.adapters.contracts.relation import (
    ComponentName,
)
from dbt.adapters.databricks.utils import remove_undefined
from dbt.adapters.utils import classproperty
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import filter_null_values

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
    MaterializedView = "materialized_view"
    External = "external"
    StreamingTable = "streaming_table"
    Unknown = "unknown"


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksInformationSchema(InformationSchema):
    quote_policy: Policy = field(default_factory=lambda: DatabricksQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: DatabricksIncludePolicy())
    quote_character: str = "`"

    def is_hive_metastore(self) -> bool:
        return is_hive_metastore(self.database)


ReferenceKey = namedtuple("ReferenceKey", "database schema identifier")


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(BaseRelation):
    type: Optional[DatabricksRelationType] = None  # type: ignore
    quote_policy: Policy = field(default_factory=lambda: DatabricksQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: DatabricksIncludePolicy())
    quote_character: str = "`"
    file_format: str = "Unknown"
    owner: str = "Unknown"
    comment: Optional[str] = None
    columns: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)
    is_temporary: bool = False

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data["path"]:
            data["path"]["database"] = None
        else:
            data["path"]["database"] = remove_undefined(data["path"]["database"])
        return data

    def has_information(self) -> bool:
        return self.file_format != "Unknown"

    def is_hive_metastore(self) -> bool:
        return is_hive_metastore(self.database)

    @property
    def is_materialized_view(self) -> bool:
        return self.type == DatabricksRelationType.MaterializedView

    @property
    def is_streaming_table(self) -> bool:
        return self.type == DatabricksRelationType.StreamingTable

    @property
    def is_delta(self) -> bool:
        return self.file_format == "delta"

    @property
    def is_hudi(self) -> bool:
        return self.file_format == "hudi"

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

    def information_schema(self, view_name: Optional[str] = None) -> InformationSchema:
        # some of our data comes from jinja, where things can be `Undefined`.
        if not isinstance(view_name, str):
            view_name = None

        # Kick the user-supplied schema out of the information schema relation
        # Instead address this as <database>.information_schema by default
        info_schema = DatabricksInformationSchema.from_relation(self, view_name)
        return info_schema.incorporate(path={"schema": None})

    @classproperty
    def StreamingTable(cls) -> str:
        return str(DatabricksRelationType.StreamingTable)

    def get_relation_key(self) -> ReferenceKey:
        return ReferenceKey(self.database, self.schema, self.identifier)


class RelationInfoCache:
    def __init__(self, data: Dict[ReferenceKey, DatabricksRelation] = {}):
        self.cache: Dict[ReferenceKey, DatabricksRelation] = {}
        self.lock = threading.Lock()

    def get(self, key: ReferenceKey) -> Optional[DatabricksRelation]:
        with self.lock:
            return self.cache.get(key)

    def set(self, relation: DatabricksRelation) -> None:
        with self.lock:
            self.cache[relation.get_relation_key()] = relation

    def set_all(self, relations: List[DatabricksRelation]) -> None:
        with self.lock:
            for relation in relations:
                self.cache[relation.get_relation_key()] = relation

    def delete(self, key: ReferenceKey) -> None:
        with self.lock:
            self.cache.pop(key, None)

    def clear(self) -> None:
        with self.lock:
            self.cache.clear()


def is_hive_metastore(database: Optional[str]) -> bool:
    return database is None or database.lower() == "hive_metastore"


def extract_identifiers(relations: Iterable[BaseRelation]) -> Set[str]:
    return {r.identifier for r in relations if r.identifier is not None}
