from jinja2 import Template
from mock import MagicMock
from tests.unit.macros.base import MacroTestBase

import pytest


class TestPythonMacros(MacroTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def modify_context(self, default_context) -> None:
        default_context["model"] = MagicMock()
        d = {"alias": "schema"}
        default_context["model"].__getitem__.side_effect = d.__getitem__

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/adapters"]

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "python.sql"

    def test_py_get_writer__default_file_format(self, template):
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("delta")'

    def test_py_get_writer__specified_file_format(self, config, template):
        config["file_format"] = "parquet"
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("parquet")'

    def test_py_get_writer__specified_location_root(self, config, template, context):
        config["location_root"] = "s3://fake_location"
        result = self.run_macro_raw(template, "py_get_writer_options")

        expected = '.format("delta")\n.option("path", "s3://fake_location/schema")'
        assert result == expected

    def test_py_get_writer__specified_location_root_on_incremental(
        self, config, template: Template, context
    ):
        config["location_root"] = "s3://fake_location"
        context["is_incremental"].return_value = True
        result = self.run_macro_raw(template, "py_get_writer_options")

        expected = '.format("delta")\n.option("path", "s3://fake_location/schema__dbt_tmp")'
        assert result == expected

    def test_py_get_writer__partition_by_single_column(self, config, template):
        config["partition_by"] = "name"
        result = self.run_macro_raw(template, "py_get_writer_options")

        expected = ".format(\"delta\")\n.partitionBy(['name'])"
        assert result == expected

    def test_py_get_writer__partition_by_array(self, config, template):
        config["partition_by"] = ["name", "date"]
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == ".format(\"delta\")\n.partitionBy(['name', 'date'])"

    def test_py_get_writer__clustered_by_single_column(self, config, template):
        config["clustered_by"] = "name"
        config["buckets"] = 2
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == ".format(\"delta\")\n.bucketBy(2, ['name'])"

    def test_py_get_writer__clustered_by_array(self, config, template):
        config["clustered_by"] = ["name", "date"]
        config["buckets"] = 2
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == ".format(\"delta\")\n.bucketBy(2, ['name', 'date'])"

    def test_py_get_writer__clustered_by_without_buckets(self, config, template):
        config["clustered_by"] = ["name", "date"]
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("delta")'

    def test_py_try_import__golden_path(self, template):
        result = self.run_macro_raw(template, "py_try_import", "pandas", "pandas_available")

        expected = (
            "# make sure pandas exists before using it\n"
            "try:\n"
            "    import pandas\n"
            "    pandas_available = True\n"
            "except ImportError:\n"
            "    pandas_available = False\n"
        )
        assert result == expected
