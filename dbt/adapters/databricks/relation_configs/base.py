from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Dict, Generic, List, TypeVar

from dbt.adapters.relation_configs.config_base import RelationConfigBase, RelationResults
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.relation_configs.config_change import RelationConfigChange


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksComponentConfig(ABC):
    @abstractmethod
    def to_sql_clause(self) -> str:
        raise NotImplementedError("Must be implemented by subclass")


Component = TypeVar("Component", bound=DatabricksComponentConfig)


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


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationConfigBase(RelationConfigBase):
    config_components: ClassVar[List[type[DatabricksComponentProcessor]]]

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


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationChangeSet:
    changes: List[RelationConfigChange]

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self.changes)

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0
