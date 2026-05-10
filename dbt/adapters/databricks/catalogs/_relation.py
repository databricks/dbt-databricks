import posixpath
from dataclasses import dataclass
from typing import Optional

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks import constants


@dataclass
class DatabricksCatalogRelation:
    catalog_type: str = constants.DEFAULT_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_CATALOG.name
    table_format: Optional[str] = constants.DEFAULT_CATALOG.table_format
    file_format: Optional[str] = constants.DEFAULT_CATALOG.file_format
    external_volume: Optional[str] = constants.DEFAULT_CATALOG.external_volume
    location_path: Optional[str] = None

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

    @property
    def location(self) -> Optional[str]:
        if self.location_root and self.location_path:
            return posixpath.join(self.location_root, self.location_path)
        return None

    @property
    def iceberg_table_properties(self) -> dict[str, str]:
        if self.table_format == constants.ICEBERG_TABLE_FORMAT:
            if self.file_format != constants.DELTA_FILE_FORMAT:
                raise DbtConfigError(
                    "When table_format is 'iceberg', cannot set file_format to other than delta."
                )
            return {
                "delta.enableIcebergCompatV2": "true",
                "delta.universalFormat.enabledFormats": constants.ICEBERG_TABLE_FORMAT,
            }
        return {}
