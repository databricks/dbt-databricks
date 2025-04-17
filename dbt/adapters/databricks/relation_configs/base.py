from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults


class DatabricksComponentConfig(BaseModel):
    """Class for encapsulating a single component of a Databricks relation config.

    Ex: A materialized view has a `query` component, which is a string that if changed, requires a
    full refresh.
    """

    model_config = ConfigDict(frozen=True)

    def get_diff(self, other: Self) -> Optional[Self]:
        """Get the config that must be applied when this component differs from the existing
        version. This method is intended to only be called on the new version (i.e. the version
        specified in the dbt project).

        If the difference does not require any changes to the existing relation, this method should
        return None. If some partial change can be applied to the existing relation, the
        implementing component should override this method to return an instance representing the
        partial change; however, care should be taken to ensure that the returned object retains
        the complete config specified in the dbt project, so as to support rendering the `create`
        as well as the `alter` statements, for the case where a different component requires full
        refresh.
        """

        if self != other:
            return self
        return None


class DatabricksRelationChangeSet(BaseModel):
    """Class for encapsulating the changes that need to be applied to a Databricks relation."""

    changes: dict[str, DatabricksComponentConfig] = Field(frozen=True)
    requires_full_refresh: bool = Field(default=False, frozen=False)

    @property
    def has_changes(self) -> bool:
        """Whether or not this change set has any changes that need to be applied."""

        return len(self.changes) > 0


Component = TypeVar("Component", bound=DatabricksComponentConfig)


class DatabricksComponentProcessor(ABC, Generic[Component]):
    """Class for encapsulating the logic for extracting a single config component from either the
    project config, or the existing relation.
    """

    # The name of the component. This is used as the key in the config dictionary of the relation
    # config.
    name: ClassVar[str]

    @classmethod
    @abstractmethod
    def from_relation_results(cls, row: RelationResults) -> Component:
        """Extract the component from the results of a query against the existing relation."""

        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    @abstractmethod
    def from_relation_config(cls, model_node: RelationConfig) -> Component:
        """Extract the component from the model node.

        While some components, e.g. query, can be extracted directly from the model node,
        specialized Databricks config can be found in model_node.config.extra.
        """

        raise NotImplementedError("Must be implemented by subclass")


class DatabricksRelationConfigBase(BaseModel, ABC):
    """Class for encapsulating the config of a Databricks relation.

    Ex: all of the config for specifying a Materialized View is handled by MaterializedViewConfig.
    Concretely though, since that config is compatible with the default behavior of this class,
    only the list of component processors is specified by its subclass.
    """

    # The list of components that make up the relation config. In the base implemenation, these
    # components are applied sequentially to either the existing relation, or the model node, to
    # build up the config.
    config_components: ClassVar[list[type[DatabricksComponentProcessor]]]
    config: dict[str, DatabricksComponentConfig]

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> Self:
        """Build the relation config from a model node."""

        config_dict: dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            relation_component = component.from_relation_config(relation_config)
            if relation_component:
                config_dict[component.name] = relation_component

        return cls(config=config_dict)

    @classmethod
    def from_results(cls, results: RelationResults) -> Self:
        """Build the relation config from the results of a query against the existing relation."""

        config_dict: dict[str, DatabricksComponentConfig] = {}
        for component in cls.config_components:
            result_component = component.from_relation_results(results)
            if result_component:
                config_dict[component.name] = result_component

        return cls(config=config_dict)

    def get_changeset(self, existing: Self) -> Optional[DatabricksRelationChangeSet]:
        """Get the changeset that must be applied to the existing relation to make it match the
        current state of the dbt project. If no changes are required, this method should return
        None.
        """
        changes: dict[str, DatabricksComponentConfig] = {}

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if diff:
                changes[key] = diff

        if len(changes) > 0:
            return DatabricksRelationChangeSet(changes=changes, requires_full_refresh=False)
        return None


DatabricksRelationConfig = TypeVar("DatabricksRelationConfig", bound=DatabricksRelationConfigBase)


def get_config_value(config: RelationConfig, key: str) -> Any:
    """Get a value from the config.extra dictionary, or None if it is not present."""

    materialization_config = config.config
    if materialization_config:
        return materialization_config.extra.get(key)
    return None
