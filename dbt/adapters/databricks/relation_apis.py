from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional

from dbt.adapters.databricks import macros, utils
from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksRelationType
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfig
from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.materialized_view import MaterializedViewConfig
from dbt.adapters.databricks.relation_configs.streaming_table import StreamingTableConfig
from dbt.adapters.databricks.relation_configs.view import ViewConfig
from dbt.adapters.relation_configs import RelationResults
from dbt.adapters.sql import SQLAdapter

if TYPE_CHECKING:
    from dbt.adapters.contracts.relation import RelationConfig


@dataclass(frozen=True)
class RelationConfigFactoryBase(ABC, Generic[DatabricksRelationConfig]):
    """Base class for relation config factories.
    These factories create relation config objects from various sources.
    """

    relation_type: ClassVar[str]

    @classmethod
    @abstractmethod
    def config_type(cls) -> type[DatabricksRelationConfig]:
        """Get the config class for delegating calls."""

        raise NotImplementedError("Must be implemented by subclass")

    @classmethod
    def get_from_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> DatabricksRelationConfig:
        """Get the relation config from the relation."""

        assert relation.type == cls.relation_type
        results = cls._describe_relation(adapter, relation, api_client)
        return cls.config_type().from_results(results)

    @classmethod
    def get_from_relation_config(
        cls, relation_config: "RelationConfig"
    ) -> DatabricksRelationConfig:
        """Get the relation config from the model node."""

        return cls.config_type().from_relation_config(relation_config)

    @classmethod
    @abstractmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        """Describe the relation and return the results."""

        raise NotImplementedError("Must be implemented by subclass")


class DeltaLiveTableConfigFactoryBase(RelationConfigFactoryBase[DatabricksRelationConfig]):
    @classmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        # Poll for DLT completion if API client is provided
        if api_client is not None:
            api_client.dlt_pipelines.poll_for_completion(relation.identifier)

        kwargs = {"table_name": relation}
        results: RelationResults = dict()
        results["describe_extended"] = adapter.execute_macro(
            macros.DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )
        return results


class MaterializedViewConfigFactory(DeltaLiveTableConfigFactoryBase[MaterializedViewConfig]):
    relation_type = DatabricksRelationType.MaterializedView

    @classmethod
    def config_type(cls) -> type[MaterializedViewConfig]:
        return MaterializedViewConfig

    @classmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        kwargs = {"table_name": relation}
        results: RelationResults = dict()
        results["describe_extended"] = adapter.execute_macro(
            macros.DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )

        kwargs = {"relation": relation}
        results["information_schema.views"] = utils.get_first_row(
            adapter.execute_macro("get_view_description", kwargs=kwargs)
        )
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results


class StreamingTableConfigFactory(DeltaLiveTableConfigFactoryBase[StreamingTableConfig]):
    relation_type = DatabricksRelationType.StreamingTable

    @classmethod
    def config_type(cls) -> type[StreamingTableConfig]:
        return StreamingTableConfig

    @classmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        kwargs = {"table_name": relation}
        results: RelationResults = dict()
        results["describe_extended"] = adapter.execute_macro(
            macros.DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs=kwargs
        )

        kwargs = {"relation": relation}
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)
        return results


class IncrementalTableConfigFactory(RelationConfigFactoryBase[IncrementalTableConfig]):
    relation_type = DatabricksRelationType.Table

    @classmethod
    def config_type(cls) -> type[IncrementalTableConfig]:
        return IncrementalTableConfig

    @classmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        kwargs = {"relation": relation}
        results: RelationResults = dict()

        # DLT incremental tables don't have tags, column tags, constraints,  etc.
        # TODO: Add is_delta_live_table property to DatabricksRelation
        if not relation.is_hive_metastore():  # Only fetch Unity Catalog features for UC relations
            results["information_schema.tags"] = adapter.execute_macro("fetch_tags", kwargs=kwargs)
            results["information_schema.column_tags"] = adapter.execute_macro(
                "fetch_column_tags", kwargs=kwargs
            )
            results["non_null_constraint_columns"] = adapter.execute_macro(
                "fetch_non_null_constraint_columns", kwargs=kwargs
            )
            results["primary_key_constraints"] = adapter.execute_macro(
                "fetch_primary_key_constraints", kwargs=kwargs
            )
            results["foreign_key_constraints"] = adapter.execute_macro(
                "fetch_foreign_key_constraints", kwargs=kwargs
            )
            results["column_masks"] = adapter.execute_macro("fetch_column_masks", kwargs=kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)

        results["describe_extended"] = adapter.execute_macro(
            macros.DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": relation}
        )
        return results


class ViewConfigFactory(RelationConfigFactoryBase[ViewConfig]):
    relation_type = DatabricksRelationType.View

    @classmethod
    def config_type(cls) -> type[ViewConfig]:
        return ViewConfig

    @classmethod
    def _describe_relation(
        cls, adapter: SQLAdapter, relation: DatabricksRelation, api_client: Optional[Any] = None
    ) -> RelationResults:
        kwargs = {"relation": relation}
        results: RelationResults = dict()

        results["information_schema.views"] = utils.get_first_row(
            adapter.execute_macro("get_view_description", kwargs=kwargs)
        )
        if not relation.is_hive_metastore():  # Only fetch tags for Unity Catalog relations
            results["information_schema.tags"] = adapter.execute_macro("fetch_tags", kwargs=kwargs)
        results["show_tblproperties"] = adapter.execute_macro("fetch_tbl_properties", kwargs=kwargs)

        results["describe_extended"] = adapter.execute_macro(
            macros.DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": relation}
        )
        return results
