from dataclasses import dataclass
from typing import Optional

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtValidationError


@dataclass
class UnityDatabricksConfig(dbtClassMixin):
    file_format: str
    location_root: Optional[str] = None
    use_uniform: Optional[bool] = None

    def __post_init__(self) -> None:
        if not self.file_format.strip():
            raise DbtValidationError("'file_format' must be non-empty")
        # file_format depends on use_uniform — see dbt-adapters issue #9648
        if self.use_uniform:
            if self.file_format.lower() != "delta":
                raise DbtValidationError("file_format must be 'delta' when 'use_uniform' is true")
        else:
            if self.file_format.lower() != "parquet":
                raise DbtValidationError(
                    "file_format must be 'parquet' when 'use_uniform' is false or unset"
                )
        if self.location_root is not None and not self.location_root.strip():
            raise DbtValidationError("'location_root' cannot be blank")


@dataclass
class HiveMetastoreDatabricksConfig(dbtClassMixin):
    file_format: str

    def __post_init__(self) -> None:
        if self.file_format.lower() not in {"delta", "parquet", "hudi"}:
            raise DbtValidationError(
                f"file_format must be one of: {sorted(f.upper() for f in {'delta', 'parquet', 'hudi'})}"
            )