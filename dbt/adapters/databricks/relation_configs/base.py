from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Dict, Generic, List, Optional, TypeVar
from typing_extensions import Self, Type

from dbt.adapters.relation_configs.config_base import RelationConfigBase, RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.relation_configs.config_change import (
    RelationConfigChange,
    RelationConfigChangeAction,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksComponentConfig(ABC):
    @abstractmethod
    def to_sql_clause(self) -> str:
        raise NotImplementedError("Must be implemented by subclass")

    def get_diff(self, other: "DatabricksComponentConfig") -> Self:
        if not isinstance(other, self.__class__):
            raise DbtRuntimeError(
                f"Cannot diff {self.__class__.__name__} with {other.__class__.__name__}"
            )
        return self


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksAlterableComponentConfig(DatabricksComponentConfig, ABC):
    @abstractmethod
    def to_alter_sql_clauses(self) -> List[str]:
        raise NotImplementedError("Must be implemented by subclass")


Component = TypeVar("Component", bound=DatabricksComponentConfig)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksConfigChange(RelationConfigChange, Generic[Component]):
    context: Component

    @property
    def requires_full_refresh(self) -> bool:
        return not isinstance(self.context, DatabricksAlterableComponentConfig)

    @classmethod
    def get_change(cls, new: Component, existing: Component) -> Optional[Self]:
        if new != existing:
            return cls(RelationConfigChangeAction.alter, new.get_diff(existing))
        return None


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationChangeSet:
    changes: List[DatabricksConfigChange]

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self.changes)

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0

    def get_alter_sql_clauses(self) -> List[str]:
        assert (
            not self.requires_full_refresh
        ), "Cannot alter a relation when changes requires a full refresh"
        return [
            clause for change in self.changes for clause in change.context.to_alter_sql_clauses()
        ]


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksComponentProcessor(ABC, Generic[Component]):
    name: ClassVar[str]

    @classmethod
    @abstractmethod
    def from_results(cls, row: RelationResults) -> Component:
        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    @abstractmethod
    def from_model_node(cls, model_node: ModelNode) -> Component:
        raise NotImplementedError("Must be implemented by subclass")


T = TypeVar("T", bound=DatabricksComponentConfig)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationConfigBase(RelationConfigBase):
    config_components: ClassVar[List[Type[DatabricksComponentProcessor]]]

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> RelationConfigBase:
        config_dict: Dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            config_dict[component.name] = component.from_model_node(model_node)

        return cls.from_dict(config_dict)

    @classmethod
    def from_results(cls, results: RelationResults) -> RelationConfigBase:
        config_dict: Dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            config_dict[component.name] = component.from_results(results)

        return cls.from_dict(config_dict)

    @abstractmethod
    def get_changeset(self, existing: Self) -> Optional[DatabricksRelationChangeSet]:
        raise NotImplementedError("Must be implemented by subclass")
