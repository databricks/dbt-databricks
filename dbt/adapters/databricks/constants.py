from types import SimpleNamespace


DEFAULT_TABLE_FORMAT = "default"


DEFAULT_FILE_FORMAT = "parquet"


HIVE_METASTORE_CATALOG_TYPE = "hive_metastore"


DEFAULT_BUILT_IN_CATALOG = SimpleNamespace(
    name="hive_metastore",
    catalog_type="managed",
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
