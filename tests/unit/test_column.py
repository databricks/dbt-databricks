import unittest

from dbt.adapters.databricks import DatabricksColumn


class TestSparkColumn(unittest.TestCase):

    def test_convert_table_stats_with_no_statistics(self):
        self.assertDictEqual(
            DatabricksColumn.convert_table_stats(None),
            {}
        )

    def test_convert_table_stats_with_bytes(self):
        self.assertDictEqual(
            DatabricksColumn.convert_table_stats("123456789 bytes"),
            {
                'stats:bytes:description': '',
                'stats:bytes:include': True,
                'stats:bytes:label': 'bytes',
                'stats:bytes:value': 123456789
            }
        )

    def test_convert_table_stats_with_bytes_and_rows(self):
        self.assertDictEqual(
            DatabricksColumn.convert_table_stats("1234567890 bytes, 12345678 rows"),
            {
                'stats:bytes:description': '',
                'stats:bytes:include': True,
                'stats:bytes:label': 'bytes',
                'stats:bytes:value': 1234567890,
                'stats:rows:description': '',
                'stats:rows:include': True,
                'stats:rows:label': 'rows',
                'stats:rows:value': 12345678
            }
        )
