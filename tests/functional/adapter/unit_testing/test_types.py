import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["2.0", "2.0"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'1'::numeric", "1"],
            ["array(1, 2, 3)", "'array(1, 2, 3)'"],
            [
                "map('10', 't', '15', 'f', '20', NULL)",
                """'map("10", "t", "15", "f", "20", NULL)'""",
            ],
            [
                'named_struct("a", 1, "b", 2, "c", 3)',
                """'named_struct("a", 1, "b", 2, "c", 3)'""",
            ],
        ]
