import pprint
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from dbt.task.debug import DebugTask

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


def _debug_output(credentials):
    task = DebugTask.__new__(DebugTask)
    task.profile = SimpleNamespace(credentials=credentials)
    with (
        patch("dbt.task.debug.fire_event") as fire_event,
        patch.object(DebugTask, "attempt_connection", return_value=None),
    ):
        task.test_connection()
    return "\n".join(call.args[0].msg for call in fire_event.call_args_list)


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
    def test_debug_output_redacts_nested_credentials(self, session_properties, expected):
        output = _debug_output(_credentials(session_properties))
        assert f"session_properties: {expected}" in output
        assert "secret-value" not in output
        assert "top-level-token" not in output

    def test_redaction_does_not_mutate_credentials(self):
        credentials = _credentials({"access_token": "secret-value", "nested": [{"secret": "s"}]})
        _debug_output(credentials)
        assert credentials.session_properties == {
            "access_token": "secret-value",
            "nested": [{"secret": "s"}],
        }

    @pytest.mark.parametrize(
        "key", ["spark.hadoop.fs.s3a.aws.credentials.provider", "access_token", "ansi_mode"]
    )
    def test_profile_hash_input_tracks_every_session_property(self, key):
        # dbt-core hashes `pprint.pformat` of `connection_info()` to decide when to reparse.
        def hash_input(value):
            return pprint.pformat(sorted(_credentials({key: value}).connection_info()))

        assert hash_input("one") != hash_input("two")
        assert hash_input("one") == pprint.pformat(
            sorted(
                [
                    ("host", "example.invalid"),
                    ("http_path", "/sql/1.0/warehouses/abc"),
                    ("catalog", "main"),
                    ("schema", "analytics"),
                    ("session_properties", {key: "one"}),
                ]
            )
        )

    def test_target_context_keeps_raw_session_properties(self):
        credentials = _credentials({"access_token": "secret-value"})
        info = dict(credentials.connection_info(with_aliases=True))
        assert info["session_properties"] == {"access_token": "secret-value"}
