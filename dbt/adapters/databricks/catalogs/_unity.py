from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks import constants, parse_model
from dbt.adapters.databricks.catalogs._relation import DatabricksCatalogRelation


class UnityCatalogIntegration(CatalogIntegration):
    catalog_type = constants.UNITY_CATALOG_TYPE
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        if location_root := config.adapter_properties.get("location_root"):
            self.external_volume: Optional[str] = location_root
        self.file_format: Optional[str] = config.file_format

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
        return DatabricksCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name
            if self.catalog_name != constants.DEFAULT_CATALOG.catalog_name
            else model.database,
            table_format=parse_model.table_format(model) or self.table_format,
            file_format=parse_model.file_format(model) or self.file_format,
            external_volume=parse_model.location_root(model) or self.external_volume,
            location_path=parse_model.location_path(model),
        )
