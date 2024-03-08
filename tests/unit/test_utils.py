import unittest

from dbt.adapters.databricks.utils import redact_credentials, remove_ansi


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
            "    credential ('KEY_1' = 'VALUE=1**asa!??sh', 'KEY_2' = 'VALUE2')\n"
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
            "fileformat = parquet\n"
            "format_options ('mergeSchema' = 'True')\n"
            "copy_options ('mergeSchema' = 'True')"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY1' = '[REDACTED]', 'KEY2' = '[REDACTED]', 'KEY3' = '[REDACTED]')\n"
            "  )\n"
            "fileformat = parquet\n"
            "format_options ('mergeSchema' = 'True')\n"
            "copy_options ('mergeSchema' = 'True')"
        )
        self.assertEqual(redact_credentials(sql), expected)

    def test_remove_ansi(self):
        test_string = """Python model failed with traceback as:
  [0;31m---------------------------------------------------------------------------[0m
  [0;31mException[0m                                 Traceback (most recent call last)
  File [0;32m~/.ipykernel/1292/command--1-4090367456:79[0m
  [1;32m     70[0m [38;5;66;03m# COMMAND ----------[39;00m
  [1;32m     71[0m
  [1;32m     72[0m [38;5;66;03m# how to execute python model in notebook[39;00m
"""
        expected_string = """Python model failed with traceback as:
  ---------------------------------------------------------------------------
  Exception                                 Traceback (most recent call last)
  File ~/.ipykernel/1292/command--1-4090367456:79
       70 # COMMAND ----------
       71
       72 # how to execute python model in notebook
"""
        self.assertEqual(remove_ansi(test_string), expected_string)
