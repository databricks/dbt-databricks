import dbt.adapters.databricks.utils as databricks_utils
from dbt.adapters.databricks.utils import (
    is_cluster_http_path,
    quote,
    redact_credentials,
    remove_ansi,
)


class TestDatabricksUtils:
    def test_redact_credentials__no_credentials(self):
        sql = "copy into target_table\nfrom source_table\nfileformat = parquet"
        expected = sql
        assert redact_credentials(sql) == expected

    def test_redact_credentials__single_credential(self):
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

        assert redact_credentials(sql) == expected

    def test_redact_credentials__multiple_credentials(self):
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
        assert redact_credentials(sql) == expected

    def test_redact_credentials__uppercase_credential(self):
        sql = "copy into target_table\nfrom source_table\n  WITH (CREDENTIAL ('KEY' = 'VALUE'))"
        expected = (
            "copy into target_table\nfrom source_table\n  WITH (CREDENTIAL ('KEY' = '[REDACTED]'))"
        )
        assert redact_credentials(sql) == expected

    def test_redact_credentials__encryption(self):
        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (encryption ('TYPE' = 'AWS_SSE_C', 'MASTER_KEY' = 'VALUE'))"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (encryption ('TYPE' = '[REDACTED]', 'MASTER_KEY' = '[REDACTED]'))"
        )
        assert redact_credentials(sql) == expected

    def test_redact_credentials__credential_and_encryption(self):
        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (credential ('KEY' = 'VALUE') encryption ('MASTER_KEY' = 'VALUE'))"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (credential ('KEY' = '[REDACTED]') encryption ('MASTER_KEY' = '[REDACTED]'))"
        )
        assert redact_credentials(sql) == expected

    def test_redact_credentials__value_with_comma(self):
        sql = "copy into target_table\n  WITH (credential ('KEY' = 'VALUE,WITH,COMMAS'))"
        expected = "copy into target_table\n  WITH (credential ('KEY' = '[REDACTED]'))"
        assert redact_credentials(sql) == expected

    def test_redact_credentials__value_with_newline(self):
        sql = "copy into target_table\n  WITH (credential ('KEY' = 'VALUE\nCONTINUED'))"
        expected = "copy into target_table\n  WITH (credential ('KEY' = '[REDACTED]'))"
        assert redact_credentials(sql) == expected

    def test_redact_credentials__value_with_quote(self):
        sql = "copy into target_table\n  WITH (credential ('KEY' = 'VALUE'WITH'QUOTES'))"
        expected = "copy into target_table\n  WITH (credential ('KEY' = '[REDACTED]'))"
        assert redact_credentials(sql) == expected

    def test_redact_credentials__value_with_escaped_quote(self):
        sql = "copy into target_table\n  WITH (credential ('KEY' = 'VALUE\\'ESCAPED'))"
        expected = "copy into target_table\n  WITH (credential ('KEY' = '[REDACTED]'))"
        assert redact_credentials(sql) == expected

    def test_redact_credentials__escaped_quote_before_delimiter(self):
        cases = [
            "copy into target_table\n  WITH (credential ('KEY' = 'PREFIX'',SUFFIX'))",
            "copy into target_table\n  WITH (credential ('KEY' = 'PREFIX'')SUFFIX'))",
            "copy into target_table\n  WITH (credential ('KEY' = 'PREFIX\\',SUFFIX'))",
            "copy into target_table\n  WITH (credential ('KEY' = 'PREFIX\\')SUFFIX'))",
        ]
        expected = "copy into target_table\n  WITH (credential ('KEY' = '[REDACTED]'))"

        for sql in cases:
            redacted = redact_credentials(sql)
            assert redacted == expected
            assert "PREFIX" not in redacted
            assert "SUFFIX" not in redacted

    def test_redact_credentials__malformed_secret_clause_is_unchanged(self):
        sql = "copy into target_table\n  WITH (credential ('KEY' = 'PREFIX',SUFFIX')) trailing SQL"

        assert redact_credentials(sql) == sql

    def test_redact_credentials__secretless_clause_is_unchanged(self):
        cases = [
            "copy into target_table WITH (credential ())",
            "select credential('public literal') as x, 42 as y",
            "select my_encryption('public literal') as x",
        ]

        for sql in cases:
            assert redact_credentials(sql) == sql

    def test_redact_credentials__unquoted_key_is_unchanged(self):
        sql = "copy into target_table WITH (credential (KEY = 'SECRET')) trailing SQL"

        assert redact_credentials(sql) == sql

    def test_redact_credentials__large_unterminated_clause(self):
        sql = "credential (" + ", ".join("'KEY' = 'VALUE'" for _ in range(1_000))

        assert redact_credentials(sql) == sql

    def test_redact_credentials__large_ordinary_statement_uses_fast_path(self, monkeypatch):
        class UnexpectedRegex:
            def sub(self, replacement, sql):
                raise AssertionError("ordinary SQL should bypass the secret-clause regex")

        monkeypatch.setattr(databricks_utils, "SECRET_CLAUSE_IN_COPY_INTO_REGEX", UnexpectedRegex())
        sql = "select 1 -- " + "x" * 1_000_000

        assert redact_credentials(sql) == sql

    def test_redact_credentials__internal_error_fails_open(self, monkeypatch):
        sql = "copy into target_table WITH (credential ('KEY' = 'SYNTHETIC_SECRET'))"

        def raise_internal_error(sql: str) -> str:
            raise RuntimeError("synthetic redactor failure")

        monkeypatch.setattr(
            databricks_utils,
            "_redact_credentials_in_copy_into",
            raise_internal_error,
        )

        assert redact_credentials(sql) == sql

    def test_redact_credentials__key_with_dots(self):
        sql = "copy into target_table\n  WITH (credential ('fs.azure.account.key' = 'VALUE'))"
        expected = (
            "copy into target_table\n  WITH (credential ('fs.azure.account.key' = '[REDACTED]'))"
        )
        assert redact_credentials(sql) == expected

    def test_redact_credentials__prefixed_keyword(self):
        sql = "copy into target_table\n  WITH (storage_credential ('KEY' = 'VALUE'))"
        expected = "copy into target_table\n  WITH (storage_credential ('KEY' = '[REDACTED]'))"
        assert redact_credentials(sql) == expected

    def test_redact_credentials__non_option_clause(self):
        sql = "select * from target_table where credential_id = 1"
        assert redact_credentials(sql) == sql

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
        assert remove_ansi(test_string) == expected_string

    def test_quote(self):
        assert quote("table") == "`table`"

    def test_is_cluster_http_path_with_cluster_id(self):
        assert is_cluster_http_path("/sql/1.0/warehouses/abc", "cluster-123") is False

    def test_is_cluster_http_path_without_cluster_id_and_warehouses(self):
        assert is_cluster_http_path("/sql/1.0/endpoints/abc", None) is False

    def test_is_cluster_http_path_without_cluster_id_and_with_warehouses(self):
        assert is_cluster_http_path("/sql/1.0/warehouses/abc", None) is False
