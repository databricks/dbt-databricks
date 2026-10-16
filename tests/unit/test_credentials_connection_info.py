import pytest

from dbt.adapters.databricks.credentials import DatabricksCredentials


def _credentials(session_properties):
    return DatabricksCredentials.from_dict(
        {
            "database": "main",
            "schema": "analytics",
            "host": "example.invalid",
            "http_path": "/sql/1.0/warehouses/abc",
            "token": "top-level-token",
            "session_properties": session_properties,
        }
    )


class TestConnectionInfoRedaction:
    @pytest.mark.parametrize(
        "session_properties, expected",
        [
            ({"access_token": "secret-value", "ansi_mode": "true"}, {"ansi_mode": "true"}),
            ({"CLIENT_SECRET": "secret-value"}, {}),
            (
                {"outer": {"api_key": "secret-value", "keep": "ok"}},
                {"outer": {"keep": "ok"}},
            ),
            (
                {"items": [{"password": "secret-value", "keep": "ok"}]},
                {"items": [{"keep": "ok"}]},
            ),
            (
                {
                    "spark.hadoop.fs.s3a.secret.key": "secret-value",
                    "fs.s3a.session.token": "secret-value",
                    "fs.azure.account.key.acct.dfs.core.windows.net": "secret-value",
                    "spark.sql.shuffle.partitions": "8",
                },
                {"spark.sql.shuffle.partitions": "8"},
            ),
            (
                {"token_uri": "https://example.invalid/token", "access_key_id": "AKIA"},
                {"token_uri": "https://example.invalid/token", "access_key_id": "AKIA"},
            ),
        ],
    )
    def test_debug_display_redacts_nested_credentials(self, session_properties, expected):
        credentials = _credentials(session_properties)
        info = dict(credentials.connection_info())
        assert info["session_properties"] == expected
        assert "top-level-token" not in str(info)

    def test_redaction_does_not_mutate_credentials(self):
        session_properties = {"access_token": "secret-value", "nested": [{"secret": "s"}]}
        credentials = _credentials(session_properties)
        dict(credentials.connection_info())
        assert credentials.session_properties == {
            "access_token": "secret-value",
            "nested": [{"secret": "s"}],
        }

    def test_target_context_keeps_raw_session_properties(self):
        credentials = _credentials({"access_token": "secret-value"})
        info = dict(credentials.connection_info(with_aliases=True))
        assert info["session_properties"] == {"access_token": "secret-value"}
