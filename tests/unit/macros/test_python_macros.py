from mock import MagicMock
from tests.unit.macros.base import TestMacros


class TestPythonMacros(TestMacros):
    def setUp(self):
        TestMacros.setUp(self)
        self.default_context["model"] = MagicMock()
        self.template = self._get_template("python.sql", "adapters.sql")

    def test_py_get_writer__default_file_format(self):
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, '.format("delta")')

    def test_py_get_writer__specified_file_format(self):
        self.config["file_format"] = "parquet"
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, '.format("parquet")')

    def test_py_get_writer__specified_location_root(self):
        self.config["location_root"] = "s3://fake_location"
        d = {"alias": "schema"}
        self.default_context["model"].__getitem__.side_effect = d.__getitem__
        result = self._run_macro_raw("py_get_writer_options")

        expected = '.format("delta")\n.option("path", "s3://fake_location/schema")'
        self.assertEqual(result, expected)

    def test_py_get_writer__partition_by_single_column(self):
        self.config["partition_by"] = "name"
        result = self._run_macro_raw("py_get_writer_options")

        expected = ".format(\"delta\")\n.partitionBy(['name'])"
        self.assertEqual(result, expected)

    def test_py_get_writer__partition_by_array(self):
        self.config["partition_by"] = ["name", "date"]
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, (".format(\"delta\")\n.partitionBy(['name', 'date'])"))

    def test_py_get_writer__clustered_by_single_column(self):
        self.config["clustered_by"] = "name"
        self.config["buckets"] = 2
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, (".format(\"delta\")\n.bucketBy(2, ['name'])"))

    def test_py_get_writer__clustered_by_array(self):
        self.config["clustered_by"] = ["name", "date"]
        self.config["buckets"] = 2
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, (".format(\"delta\")\n.bucketBy(2, ['name', 'date'])"))

    def test_py_get_writer__clustered_by_without_buckets(self):
        self.config["clustered_by"] = ["name", "date"]
        result = self._run_macro_raw("py_get_writer_options")

        self.assertEqual(result, ('.format("delta")'))

    def test_py_try_import__golden_path(self):
        result = self._run_macro_raw("py_try_import", "pandas", "pandas_available")

        expected = (
            "# make sure pandas exists before using it\n"
            "try:\n"
            "    import pandas\n"
            "    pandas_available = True\n"
            "except ImportError:\n"
            "    pandas_available = False\n"
        )
        self.assertEqual(result, expected)
