import unittest

from dbt.adapters.databricks.utils import redact_credentials


class TestDatabricksUtils(unittest.TestCase):
    def test_redact_credentials_copy_into(self):
        sql = "copy into target_table\nfrom source_table\nfileformat = parquet"
        expected = sql
        self.assertEqual(redact_credentials(sql), expected)

        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY' = 'VALUE')\n"
            "  )\n"
            "fileformat = parquet"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY' = '[REDACTED]')\n"
            "  )\n"
            "fileformat = parquet"
        )
        self.assertEqual(redact_credentials(sql), expected)

        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY_1' = 'VALUE1**asa!??sh', 'KEY_2' = 'VALUE2')\n"
            "  )\n"
            "fileformat = parquet"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY_1' = '[REDACTED]', 'KEY_2' = '[REDACTED]')\n"
            "  )\n"
            "fileformat = parquet"
        )
        self.assertEqual(redact_credentials(sql), expected)

        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential (\n"
            "      'KEY1' = 'VALUE1', 'KEY2' = 'VALUE2', 'KEY3' = 'VALUE3'\n"
            "    )\n"
            "  )\n"
            "fileformat = parquet"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY1' = '[REDACTED]', 'KEY2' = '[REDACTED]', 'KEY3' = '[REDACTED]')\n"
            "  )\n"
            "fileformat = parquet"
        )
        self.assertEqual(redact_credentials(sql), expected)
