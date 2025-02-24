from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class TestUniqueKeyDatabricks(BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDatabricksV2(MaterializationV2Mixin, BaseIncrementalUniqueKey):
    pass
