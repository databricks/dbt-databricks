from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange


class TestIncremental(BaseIncremental):
    pass


class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
