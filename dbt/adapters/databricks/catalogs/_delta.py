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
        if location := config.adapter_properties.get("location"):
            self.external_volume: Optional[str] = location
        self.table_format: str = config.adapter_properties.get("table_format")
        self.file_format: str = config.adapter_properties.get("file_format")

    @property
    def location(self) -> Optional[str]:
        return self.external_volume

    def build_relation(self, model: RelationConfig) -> DatabricksCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        return DatabricksCatalogRelation()
