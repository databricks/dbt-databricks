import pytest

from dbt.adapters.databricks.catalogs._v2 import (
    HiveMetastoreDatabricksConfig,
    UnityDatabricksConfig,
)
from dbt.adapters.databricks.impl import DatabricksAdapter
from dbt_common.exceptions import DbtValidationError


# ===== CATALOG_V2_CONFIGS class attribute =====


def test_unity_registered():
    assert DatabricksAdapter.CATALOG_V2_CONFIGS["unity"] is UnityDatabricksConfig


def test_hive_metastore_registered():
    assert (
        DatabricksAdapter.CATALOG_V2_CONFIGS["hive_metastore"] is HiveMetastoreDatabricksConfig
    )


# ===== UnityDatabricksConfig =====


def test_unity_parquet_without_uniform():
    cfg = UnityDatabricksConfig(file_format="parquet")
    assert cfg.file_format == "parquet"


def test_unity_delta_with_uniform():
    cfg = UnityDatabricksConfig(file_format="delta", use_uniform=True)
    assert cfg.file_format == "delta"
    assert cfg.use_uniform is True


def test_unity_with_location_root():
    cfg = UnityDatabricksConfig(file_format="parquet", location_root="/mnt/data")
    assert cfg.location_root == "/mnt/data"


def test_unity_delta_without_uniform_raises():
    with pytest.raises(DbtValidationError, match="file_format must be 'parquet'"):
        UnityDatabricksConfig(file_format="delta")


def test_unity_parquet_with_uniform_raises():
    with pytest.raises(DbtValidationError, match="file_format must be 'delta'"):
        UnityDatabricksConfig(file_format="parquet", use_uniform=True)


def test_unity_blank_file_format_raises():
    with pytest.raises(DbtValidationError, match="file_format.*non-empty"):
        UnityDatabricksConfig(file_format="   ")


def test_unity_blank_location_root_raises():
    with pytest.raises(DbtValidationError, match="location_root.*blank"):
        UnityDatabricksConfig(file_format="parquet", location_root="  ")


def test_unity_rejects_unknown_keys():
    with pytest.raises(Exception, match="Additional properties"):
        UnityDatabricksConfig.validate({"file_format": "parquet", "bogus": True})


# ===== HiveMetastoreDatabricksConfig =====


def test_hive_delta_valid():
    cfg = HiveMetastoreDatabricksConfig(file_format="delta")
    assert cfg.file_format == "delta"


def test_hive_parquet_valid():
    cfg = HiveMetastoreDatabricksConfig(file_format="parquet")
    assert cfg.file_format == "parquet"


def test_hive_hudi_valid():
    cfg = HiveMetastoreDatabricksConfig(file_format="hudi")
    assert cfg.file_format == "hudi"


def test_hive_invalid_file_format_raises():
    with pytest.raises(DbtValidationError, match="file_format must be one of"):
        HiveMetastoreDatabricksConfig(file_format="avro")


def test_hive_rejects_unknown_keys():
    with pytest.raises(Exception, match="Additional properties"):
        HiveMetastoreDatabricksConfig.validate({"file_format": "delta", "extra": "bad"})
