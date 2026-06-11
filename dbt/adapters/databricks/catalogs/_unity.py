from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks import constants, parse_model
from dbt.adapters.databricks.catalogs._relation import DatabricksCatalogRelation
from dbt.adapters.databricks.logging import logger


class UnityCatalogIntegration(CatalogIntegration):
    catalog_type = constants.UNITY_CATALOG_TYPE
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        location_root = config.adapter_properties.get("location_root")
        if location_root is not None:
            if not str(location_root).strip():
                raise DbtValidationError(
                    f"Catalog '{config.name}' unity/databricks location_root cannot be blank"
                )
            self.external_volume: Optional[str] = location_root
        self.file_format: Optional[str] = config.file_format
        if config.adapter_properties.get("use_uniform") is not None:
            logger.warning(
                f"Catalog '{config.name}': use_uniform is not yet supported by the adapter "
                "and has no effect. Use the use_managed_iceberg behavior flag to control "
                "Iceberg table creation. Support for use_uniform will be added in a future release."
            )

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
