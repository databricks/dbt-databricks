from dbt_common.dataclass_schema import StrEnum


class TableFormat(StrEnum):
    """
    For now we have table format separate from file format, as Iceberg support in Databricks is via
    Delta plus a compatibility layer.  We ultimately merge file formats into table format to
    simplify things for users.
    """

    DEFAULT = "default"
    ICEBERG = "iceberg"

    def __str__(self) -> str:
        return self.value
