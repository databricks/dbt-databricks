from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Optional, Type  # noqa

from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import filter_null_values

from dbt.adapters.base.relation import BaseRelation, InformationSchema, Policy
from dbt.adapters.contracts.relation import (
    ComponentName,
    RelationType,
)
from dbt.adapters.databricks import constants
from dbt.adapters.databricks.constraints import TypedConstraint, process_constraint
from dbt.adapters.databricks.utils import remove_undefined
from dbt.adapters.spark.impl import KEY_TABLE_OWNER, KEY_TABLE_STATISTICS
from dbt.adapters.utils import classproperty

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
    Foreign = "foreign"
    StreamingTable = "streaming_table"
    MetricView = "metric_view"
    Unknown = "unknown"

    def render(self) -> str:
        """Return the type formatted for SQL statements (replace underscores with spaces)"""
        return self.value.replace("_", " ").upper()


class DatabricksTableType(StrEnum):
    External = "external"
    Managed = "managed"
    ManagedShallowClone = "managed_shallow_clone"
    ExternalShallowClone = "external_shallow_clone"


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksInformationSchema(InformationSchema):
    quote_policy: Policy = field(default_factory=lambda: DatabricksQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: DatabricksIncludePolicy())
    quote_character: str = "`"

    def is_hive_metastore(self) -> bool:
        return is_hive_metastore(self.database)


@dataclass(frozen=True, eq=False, repr=False)
class DatabricksRelation(BaseRelation):
    type: Optional[DatabricksRelationType] = None  # type: ignore
    quote_policy: Policy = field(default_factory=lambda: DatabricksQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: DatabricksIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    create_constraints: list[TypedConstraint] = field(default_factory=list)
    alter_constraints: list[TypedConstraint] = field(default_factory=list)
    metadata: Optional[dict[str, Any]] = None
    renameable_relations = (DatabricksRelationType.Table, DatabricksRelationType.View)
    replaceable_relations = (
        DatabricksRelationType.Table,
        DatabricksRelationType.View,
        DatabricksRelationType.MaterializedView,
    )
    databricks_table_type: Optional[DatabricksTableType] = None
    temporary: Optional[bool] = False

    @classmethod
    def __pre_deserialize__(cls, data: dict[Any, Any]) -> dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        if "database" not in data["path"]:
            data["path"]["database"] = None
        else:
            data["path"]["database"] = remove_undefined(data["path"]["database"])
        return data

    def has_information(self) -> bool:
        return self.metadata is not None

    def is_hive_metastore(self) -> bool:
        return is_hive_metastore(self.database, self.temporary)

    @property
    def is_materialized_view(self) -> bool:
        return self.type == DatabricksRelationType.MaterializedView

    @property
    def is_streaming_table(self) -> bool:
        return self.type == DatabricksRelationType.StreamingTable

    @property
    def is_external_table(self) -> bool:
        return self.databricks_table_type == DatabricksTableType.External

    @property
    def is_dlt(self) -> bool:
        return self.is_materialized_view or self.is_streaming_table

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

    @property
    def can_be_replaced(self) -> bool:
        return (
            self.type == DatabricksRelationType.View
            or self.type == DatabricksRelationType.MaterializedView
            or (self.is_delta is True and self.type == DatabricksRelationType.Table)
        )

    @property
    def can_be_renamed(self) -> bool:
        return self.type in (DatabricksRelationType.Table, DatabricksRelationType.View)

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
    def get_relation_type(cls) -> Type[DatabricksRelationType]:  # noqa
        """Return the DatabricksRelationType class for use in tests and external code."""
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

    def add_constraint(self, constraint: TypedConstraint) -> None:
        if constraint.type == ConstraintType.check:
            self.alter_constraints.append(constraint)
        else:
            self.create_constraints.append(constraint)

    def enrich(self, constraints: list[TypedConstraint]) -> "DatabricksRelation":
        copy = self.incorporate()
        for constraint in constraints:
            copy.add_constraint(constraint)

        return copy

    def render_constraints_for_create(self) -> str:
        processed = map(process_constraint, self.create_constraints)
        return ", ".join(c for c in processed if c is not None)

    def render(self) -> str:
        return super().render().lower()

    @classmethod
    def create_from_relation_info(
        cls,
        name: str,
        kind: str,
        file_format: Optional[str],
        owner: Optional[str],
        table_type: Optional[str],
        schema_relation: "DatabricksRelation",
    ) -> "DatabricksRelation":
        """
        Create a DatabricksRelation directly from relation information data.
        This streamlined approach eliminates intermediate data representations.
        """
        # Build metadata if we have file format info
        metadata = None
        if file_format:
            metadata = {constants.KEY_TABLE_OWNER: owner, KEY_TABLE_PROVIDER: file_format}

        # Convert table_type to DatabricksTableType if available
        databricks_table_type = None
        if table_type:
            try:
                databricks_table_type = DatabricksTableType(table_type)
            except (ValueError, AttributeError):
                databricks_table_type = None

        # Convert kind to relation type - handle the conversion here
        try:
            relation_type = cls._convert_kind_to_relation_type(kind)
        except (ValueError, AttributeError):
            relation_type = DatabricksRelationType("unknown")

        return cls.create(
            database=schema_relation.database,
            schema=schema_relation.schema,
            identifier=name,
            type=RelationType(relation_type.value),
            databricks_table_type=databricks_table_type,
            metadata=metadata,
            is_delta=file_format == "delta",
        )

    @classmethod
    def _convert_kind_to_relation_type(cls, kind: str) -> DatabricksRelationType:
        """Convert a string kind to DatabricksRelationType."""
        if not kind:
            return DatabricksRelationType("unknown")

        # Get all valid enum values directly from the enum class
        kind_lower = kind.lower()
        try:
            return DatabricksRelationType(kind_lower)
        except ValueError:
            return DatabricksRelationType("unknown")


def is_hive_metastore(database: Optional[str], temporary: Optional[bool] = False) -> bool:
    return (database is None or database.lower() == "hive_metastore") and not temporary


def extract_identifiers(relations: Iterable[BaseRelation]) -> set[str]:
    return {r.identifier for r in relations if r.identifier is not None}
