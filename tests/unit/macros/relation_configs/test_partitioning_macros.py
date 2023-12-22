import pytest
from tests.unit.macros.relation_configs.base import RelationConfigTestBase


class TestPartitioningMacros(RelationConfigTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "partitioning.sql"

    def test_get_create_sql_partition_by__empty(self, template):
        s = template.get_create_sql_partition_by([])
        assert s == ""

    def test_get_create_sql_partition_by__none(self, template):
        s = template.get_create_sql_partition_by(None)
        assert s == ""

    def test_get_create_sql_partition_by__single(self, template):
        s = template.get_create_sql_partition_by(["id"])
        assert s == "PARTITIONED BY (id)"

    def test_get_create_sql_partition_by__multiple(self, template):
        s = template.get_create_sql_partition_by(["id", "value"])
        assert s == "PARTITIONED BY (id, value)"
