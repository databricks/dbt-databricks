from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)

from tests.functional.adapter.fixtures import MaterializationV2Mixin


class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    pass


class TestMergeExcludeColumnsV2(MaterializationV2Mixin, BaseMergeExcludeColumns):
    pass
