import re
import sys
from typing import Tuple
from dbt.adapters.spark.session import SessionConnectionWrapper, Connection


DBR_VERSION_REGEX = re.compile(r"([1-9][0-9]*)\.(x|0|[1-9][0-9]*)")


class DatabricksSessionConnectionWrapper(SessionConnectionWrapper):

    _is_cluster: bool
    _dbr_version: Tuple[int, int]

    def __init__(self, handle: Connection) -> None:
        super().__init__(handle)
        self._is_cluster = True
        self.cursor()

    @property
    def dbr_version(self) -> Tuple[int, int]:
        if not hasattr(self, "_dbr_version"):
            if self._is_cluster:
                with self._cursor() as cursor:
                    cursor.execute("SET spark.databricks.clusterUsageTags.sparkVersion")
                    results = cursor.fetchone()
                    if results:
                        dbr_version: str = results[1]

                m = DBR_VERSION_REGEX.search(dbr_version)
                assert m, f"Unknown DBR version: {dbr_version}"
                major = int(m.group(1))
                try:
                    minor = int(m.group(2))
                except ValueError:
                    minor = sys.maxsize
                self._dbr_version = (major, minor)
            else:
                # Assuming SQL Warehouse uses the latest version.
                self._dbr_version = (sys.maxsize, sys.maxsize)

        return self._dbr_version
