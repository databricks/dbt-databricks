import pytest
from tests.unit.macros.relation_configs.base import RelationConfigTestBase


class TestTblPropertiesMacros(RelationConfigTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "tblproperties.sql"

    def test_get_create_sql_tblproperties__empty(self, template):
        s = template.get_create_sql_tblproperties({})
        assert s == ""

    def test_get_create_sql_tblproperties__none(self, template):
        s = template.get_create_sql_tblproperties(None)
        assert s == ""

    def test_get_create_sql_tblproperties__single(self, template):
        s = template.get_create_sql_tblproperties({"key": "value"})
        assert s == "TBLPROPERTIES ('key' = 'value')"

    def test_get_create_sql_tblproperties__multiple(self, template):
        s = template.get_create_sql_tblproperties({"key": "value", "other": "other_value"})
        assert s == "TBLPROPERTIES ('key' = 'value', 'other' = 'other_value')"
