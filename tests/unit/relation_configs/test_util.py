from agate import Table, Row
import pytest
from dbt.adapters.databricks.relation_configs import util


class TestGetFirstRow:
    @pytest.mark.parametrize(
        "input,expected",
        [
            (Table([]), Row(set())),
            (Table([Row(["first", "row"]), Row(["second", "row"])]), Row(["first", "row"])),
        ],
    )
    def test_get_first_row(self, input, expected):
        first_row = util.get_first_row(input)
        assert first_row == expected
