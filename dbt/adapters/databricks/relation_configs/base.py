from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict
from typing import ClassVar, Dict, Generic, List, Optional, TypeVar
from typing_extensions import Self, Type

from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.contracts.graph.nodes import ModelNode


class DatabricksComponentConfig(BaseModel, ABC):
    model_config = ConfigDict(frozen=True)

    @property
    @abstractmethod
    def requires_full_refresh(self) -> bool:
        raise NotImplementedError("Must be implemented by subclass")

    def get_diff(self, other: Self) -> Optional[Self]:
        if self != other:
            return self
        return None


class DatabricksRelationChangeSet(BaseModel):
    model_config = ConfigDict(frozen=True)
    changes: Dict[str, DatabricksComponentConfig]

    @property
    def requires_full_refresh(self) -> bool:
        return any(change.requires_full_refresh for change in self.changes.values())

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0


Component = TypeVar("Component", bound=DatabricksComponentConfig)


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


class DatabricksRelationConfigBase(BaseModel, ABC):
    config_components: ClassVar[List[Type[DatabricksComponentProcessor]]]
    config: Dict[str, DatabricksComponentConfig]

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> Self:
        config_dict: Dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            model_component = component.from_model_node(model_node)
            if model_component:
                config_dict[component.name] = model_component

        return cls(config=config_dict)

    @classmethod
    def from_results(cls, results: RelationResults) -> Self:
        config_dict: Dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            result_component = component.from_results(results)
            if result_component:
                config_dict[component.name] = result_component

        return cls(config=config_dict)

    def get_changeset(self, existing: Self) -> Optional[DatabricksRelationChangeSet]:
        changes = {}

        for key, value in self.config.items():
            diff = value.get_diff(existing.config[key])
            if diff:
                changes[key] = diff

        if len(changes) > 0:
            return DatabricksRelationChangeSet(changes=changes)
        return None
