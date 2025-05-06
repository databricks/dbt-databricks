from dataclasses import dataclass
from typing import Optional

from dbt.adapters.databricks import constants


@dataclass
class DatabricksCatalogRelation:
    catalog_type: str = constants.DEFAULT_BUILT_IN_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_BUILT_IN_CATALOG.name
    table_format: Optional[str] = constants.DEFAULT_BUILT_IN_CATALOG.adapter_properties.get("table_format")
    file_format: Optional[str] = constants.DEFAULT_BUILT_IN_CATALOG.adapter_properties.get("file_format")
    external_volume: Optional[str] = None

    @property
    def location(self) -> Optional[str]:
        """
        Volumes in Databricks are non-tabular datasets, which is something
        different than what we mean by "external_volume" in dbt.
        However, the protocol expects "external_volume" to be set.
        """
        return self.external_volume
