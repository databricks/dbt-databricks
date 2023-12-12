from abc import ABC
from dataclasses import InitVar, dataclass
import itertools
from typing import Any, ClassVar, Dict, Generic, List, Optional, TypeVar, Union
from dbt.adapters.relation_configs.config_base import RelationConfigBase
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode

from agate import Row


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksComponentConfig(ABC):
    def to_sql_clause(self) -> str:
        raise NotImplementedError("Must be implemented by subclass")


Component = TypeVar("Component", bound=DatabricksComponentConfig)


class DatabricksComponentProcessor(ABC, Generic[Component]):
    @classmethod
    def name(cls) -> str:
        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    def description_target(cls) -> str:
        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    def process_description_row(cls, row: Row) -> Component:
        assert row[0] == cls.description_target()
        return cls.process_description_row_impl(row)

    @classmethod
    def process_description_row_impl(cls, row: Row) -> Component:
        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    def process_model_node(cls, model_node: ModelNode) -> Component:
        raise NotImplementedError("Must be implemented by subclass")


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PartitionedByConfig(DatabricksComponentConfig):
    partition_by: Optional[List[str]] = None

    def to_sql_clause(self) -> str:
        if self.partition_by:
            return f"PARTITIONED BY ({', '.join(self.partition_by)})"
        return ""


class PartitionedByProcessor:
    @classmethod
    def process_partition_rows(cls, results: List[Row]) -> PartitionedByConfig:
        cols = []
        rows = itertools.takewhile(
            lambda row: row[0],
            itertools.dropwhile(lambda row: row[0] != "# Partition Information", results),
        )
        for row in rows:
            if not row[0].startswith("# "):
                cols.append(row[0])

        return PartitionedByConfig(cols)

    @classmethod
    def process_model_node(cls, model_node: ModelNode) -> PartitionedByConfig:
        partition_by = model_node.config.extra.get("partition_by")
        if isinstance(partition_by, str):
            return PartitionedByConfig([partition_by])
        return PartitionedByConfig(partition_by)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PartitionedByConfigChange(RelationConfigChange):
    context: Optional[PartitionedByConfig] = None

    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksRelationConfigBase(RelationConfigBase, ABC):
    partitioned_by_processor: ClassVar[Optional[type[PartitionedByProcessor]]] = None
    config_components: ClassVar[List[type[DatabricksComponentProcessor]]] = []
    _indexed_processors: ClassVar[Dict[str, DatabricksComponentProcessor]] = None

    @classmethod
    def indexed_processors(cls) -> Dict[str, DatabricksComponentProcessor]:
        if not cls._indexed_processors:
            cls._indexed_processors = {
                component.description_target(): component for component in cls.config_components
            }
        return cls._indexed_processors

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> RelationConfigBase:
        config_dict: Dict[str, DatabricksComponentConfig] = {}
        if cls.partitioned_by_processor:
            config_dict["partition_by"] = cls.partitioned_by_processor.process_model_node(
                model_node
            )
        for component in cls.config_components():
            config_dict[component.name] = component.from_model_node(model_node)

        config = cls.from_dict(config_dict)
        return config

    @classmethod
    def from_describe_extended(cls, results: List[Row]) -> RelationConfigBase:
        relation_config = cls.parse_describe_extended(results)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_describe_extended(cls, results: List[Row]) -> dict[str, DatabricksComponentConfig]:
        parsed: Dict[str, DatabricksComponentConfig] = {
            component.name(): None for component in cls.config_components
        }

        if cls.partitioned_by_processor:
            parsed["partition_by"] = cls.partitioned_by_processor.process_partition_rows(results)

        # Skip to metadata
        rows = itertools.dropwhile(lambda row: row[0] != "# Detailed Table Information", results)
        indexed_processors = cls.indexed_processors()

        for row in rows:
            if row[0] in indexed_processors:
                processor = indexed_processors[row[0]]
                parsed[processor.name()] = processor.process_description_row(row)

        return parsed
