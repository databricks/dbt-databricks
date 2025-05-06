from types import SimpleNamespace


DEFAULT_TABLE_FORMAT = "default"


DEFAULT_FILE_FORMAT = "parquet"


DEFAULT_BUILT_IN_CATALOG = SimpleNamespace(
    name="hive_metastore",
    catalog_type="managed",
    external_volume=None,
    adapter_properties={
        "table_format": DEFAULT_TABLE_FORMAT,
        "file_format": DEFAULT_FILE_FORMAT,
    },
)
