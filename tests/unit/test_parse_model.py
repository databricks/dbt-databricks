from unittest.mock import Mock

from dbt.adapters.databricks.parse_model import location_root


class TestLocationRoot:
    def test_location_root_with_mixed_case(self):
        # Create a mock RelationConfig with location_root in config.extra
        model = Mock()
        model.config = {"location_root": "abfss://something.next/TEST/folder"}

        result = location_root(model)
        assert result == "abfss://something.next/TEST/folder"
