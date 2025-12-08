from types import SimpleNamespace

DEFAULT_TABLE_FORMAT = "default"
ICEBERG_TABLE_FORMAT = "iceberg"


DELTA_FILE_FORMAT = "delta"
PARQUET_FILE_FORMAT = "parquet"
HUDI_FILE_FORMAT = "hudi"


UNITY_CATALOG_TYPE = "unity"
HIVE_METASTORE_CATALOG_TYPE = "hive_metastore"


DEFAULT_UNITY_CATALOG = SimpleNamespace(
    name="unity",
    catalog_name="default_unity",
    catalog_type=UNITY_CATALOG_TYPE,
    # requires the model to specify the external_volume (location_root) property
    external_volume=None,
    table_format=DEFAULT_TABLE_FORMAT,
    adapter_properties={},
    file_format=DELTA_FILE_FORMAT,
)
DEFAULT_HIVE_METASTORE_CATALOG = SimpleNamespace(
    name="hive_metastore",
    catalog_name="default_hive_metastore",
    catalog_type=HIVE_METASTORE_CATALOG_TYPE,
    # requires the model to specify the external_volume (location_root) property
    external_volume=None,
    table_format=DEFAULT_TABLE_FORMAT,
    adapter_properties={},
    file_format=DELTA_FILE_FORMAT,
)
DEFAULT_CATALOG = DEFAULT_UNITY_CATALOG
