from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange


class TestDatabricksIncremental(BaseIncremental):
    pass


class TestDatabricksIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
