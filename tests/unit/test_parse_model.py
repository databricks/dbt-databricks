from unittest.mock import Mock

from dbt.artifacts.resources.v1.saved_query import ExportConfig, ExportDestinationType

from dbt.adapters.databricks.parse_model import catalog_name, location_root


class TestLocationRoot:
    def test_location_root_with_mixed_case(self):
        model = Mock()
        model.config = {"location_root": "abfss://something.next/TEST/folder"}

        result = location_root(model)
        assert result == "abfss://something.next/TEST/folder"


class TestGetCatalog:
    def test_get_with_non_model_config(self):
        model = Mock()
        model.config = ExportConfig(export_as=ExportDestinationType.TABLE)

        result = catalog_name(model)
        assert result == "unity"

    def test_get_with_model_config(self):
        model = Mock()
        model.config = {"catalog_name": "my_catalog"}

        result = catalog_name(model)
        assert result == "my_catalog"
