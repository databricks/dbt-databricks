from typing import Optional

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.catalogs._relation import DatabricksCatalogRelation


class DeltaCatalogIntegration(CatalogIntegration):
    catalog_type = constants.DELTA_CATALOG_TYPE
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        if location_root := config.adapter_properties.get("location_root"):
            self.external_volume: Optional[str] = location_root
        self.table_format: str = config.adapter_properties.get("table_format")
        self.file_format: str = config.adapter_properties.get("file_format")

    @property
    def location_root(self) -> Optional[str]:
        """
        Volumes in Databricks are non-tabular datasets, which is something
        different from what we mean by "external_volume" in dbt.
        However, the protocol expects "external_volume" to be set.
        """
        return self.external_volume

    @location_root.setter
    def location_root(self, value: Optional[str]) -> None:
        self.external_volume = value

    def build_relation(self, model: RelationConfig) -> DatabricksCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return DatabricksCatalogRelation()
