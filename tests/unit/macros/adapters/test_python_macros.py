from unittest.mock import MagicMock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestPythonMacros(MacroTestBase):
    @pytest.fixture(autouse=True)
    def modify_context(self, default_context) -> None:
        default_context["model"] = MagicMock()
        d = {"alias": "schema"}
        default_context["model"].__getitem__.side_effect = d.__getitem__
        default_context["adapter"].resolve_file_format.return_value = "delta"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/adapters"]

    @pytest.fixture
    def template_name(self) -> str:
        return "python.sql"

    def test_py_get_writer__default_file_format(self, template):
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("delta")'

    def test_py_get_writer__specified_file_format(self, config, template):
        template.globals["adapter"].resolve_file_format.return_value = "parquet"
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("parquet")'

    def test_py_get_writer__managed_iceberg(self, config, template):
        # regression for #1591: managed Iceberg Python models must write
        # .format("iceberg"), not the "parquet" sentinel that resolve_file_format
        # returns (which Databricks rejects with MANAGED_TABLE_FORMAT).
        config["table_format"] = "iceberg"
        template.globals["adapter"].behavior.use_managed_iceberg = True
        template.globals["adapter"].resolve_file_format.return_value = "parquet"
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("iceberg")'

    def test_py_get_writer__iceberg_without_managed_flag(self, config, template):
        # When use_managed_iceberg is off, we defer to resolve_file_format, which
        # returns "delta" (UniForm path) for table_format='iceberg'.
        config["table_format"] = "iceberg"
        template.globals["adapter"].behavior.use_managed_iceberg = False
        template.globals["adapter"].resolve_file_format.return_value = "delta"
        result = self.run_macro_raw(template, "py_get_writer_options")

        assert result == '.format("delta")'

    def test_py_get_writer__specified_location_root(self, config, template, context):
        config["location_root"] = "s3://fake_location"
        template.globals["adapter"].compute_external_path.return_value = "s3://fake_location/schema"
        result = self.run_macro_raw(template, "py_get_writer_options")

        expected = '.format("delta")\n.option("path", "s3://fake_location/schema")'
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
