from types import SimpleNamespace


BUILT_IN_TABLE_FORMAT = "default"
ICEBERG_TABLE_FORMAT = "iceberg"
DEFAULT_TABLE_FORMAT = BUILT_IN_TABLE_FORMAT


DELTA_FILE_FORMAT = "delta"
HUDI_FILE_FORMAT = "hudi"
DEFAULT_FILE_FORMAT = DELTA_FILE_FORMAT


DELTA_CATALOG_TYPE = "delta"
HIVE_METASTORE_CATALOG_TYPE = "hive_metastore"
DEFAULT_CATALOG_TYPE = DELTA_CATALOG_TYPE


DEFAULT_DELTA_CATALOG = SimpleNamespace(
    name="delta",
    catalog_type=DELTA_CATALOG_TYPE,
    external_volume=None,
    adapter_properties={
        "table_format": DEFAULT_TABLE_FORMAT,
        "file_format": DEFAULT_FILE_FORMAT,
    },
)
DEFAULT_HIVE_METASTORE_CATALOG = SimpleNamespace(
    name="hive_metastore",
    catalog_type=HIVE_METASTORE_CATALOG_TYPE,
    external_volume=None,
    adapter_properties={
        "table_format": DEFAULT_TABLE_FORMAT,
        "file_format": DEFAULT_FILE_FORMAT,
    },
)
DEFAULT_BUILT_IN_CATALOG = DEFAULT_DELTA_CATALOG
