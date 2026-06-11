from dataclasses import dataclass, field
from typing import Any, Optional

import pytest
from dbt.adapters.capability import Capability, Support
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks.catalogs import (
    HiveMetastoreCatalogIntegration,
    UnityCatalogIntegration,
)
from dbt.adapters.databricks.impl import DatabricksAdapter


@dataclass
class _Config:
    """Minimal CatalogIntegrationConfig stub for testing __init__ validation."""

    name: str = "test_cat"
    catalog_type: str = "unity"
    catalog_name: Optional[str] = None
    table_format: Optional[str] = "iceberg"
    external_volume: Optional[str] = None
    file_format: Optional[str] = None
    adapter_properties: dict[str, Any] = field(default_factory=dict)


# ===== Adapter-level =====


def test_catalogs_v2_capability_declared():
    catalogs_v2 = getattr(Capability, "CatalogsV2", None)
    if catalogs_v2 is None:
        pytest.skip("CatalogsV2 not available in this dbt-adapters version")
    cap = DatabricksAdapter._capabilities[catalogs_v2]
    assert cap.support == Support.Full


def test_v2_to_v1_type_unity():
    adapter = object.__new__(DatabricksAdapter)
    assert adapter._v2_to_v1_type("unity") == "unity"


def test_v2_to_v1_type_hive_metastore():
    adapter = object.__new__(DatabricksAdapter)
    assert adapter._v2_to_v1_type("hive_metastore") == "hive_metastore"


def test_v2_to_v1_type_unknown_passthrough():
    adapter = object.__new__(DatabricksAdapter)
    assert adapter._v2_to_v1_type("custom_type") == "custom_type"


# ===== UnityCatalogIntegration =====


def test_unity_parquet_without_uniform():
    cfg = _Config(file_format="parquet")
    integration = UnityCatalogIntegration(cfg)
    assert integration.file_format == "parquet"


def test_unity_with_location_root():
    cfg = _Config(file_format="parquet", adapter_properties={"location_root": "/mnt/data"})
    integration = UnityCatalogIntegration(cfg)
    assert integration.external_volume == "/mnt/data"


def test_unity_blank_location_root_raises():
    cfg = _Config(file_format="parquet", adapter_properties={"location_root": "  "})
    with pytest.raises(DbtValidationError, match="location_root cannot be blank"):
        UnityCatalogIntegration(cfg)


def test_unity_empty_location_root_raises():
    cfg = _Config(file_format="parquet", adapter_properties={"location_root": ""})
    with pytest.raises(DbtValidationError, match="location_root cannot be blank"):
        UnityCatalogIntegration(cfg)


# ===== HiveMetastoreCatalogIntegration =====


def test_hive_delta_valid():
    cfg = _Config(catalog_type="hive_metastore", file_format="delta")
    integration = HiveMetastoreCatalogIntegration(cfg)
    assert integration.file_format == "delta"


def test_hive_parquet_valid():
    cfg = _Config(catalog_type="hive_metastore", file_format="parquet")
    integration = HiveMetastoreCatalogIntegration(cfg)
    assert integration.file_format == "parquet"


def test_hive_hudi_valid():
    cfg = _Config(catalog_type="hive_metastore", file_format="hudi")
    integration = HiveMetastoreCatalogIntegration(cfg)
    assert integration.file_format == "hudi"


def test_hive_invalid_file_format_raises():
    cfg = _Config(catalog_type="hive_metastore", file_format="avro")
    with pytest.raises(DbtValidationError, match="file_format"):
        HiveMetastoreCatalogIntegration(cfg)
