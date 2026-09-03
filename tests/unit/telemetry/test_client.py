from types import SimpleNamespace

import pytest

from dbt.adapters.databricks.telemetry import client


def _ack(**overrides):
    payload = {"numProtoSuccess": 1, "errors": None}
    payload.update(overrides)
    return SimpleNamespace(status_code=200, json=lambda: payload)


@pytest.mark.parametrize(
    ("workspace_id", "expected_header"),
    [("42", "42"), ("customer-name", None)],
)
def test_workspace_id_header(monkeypatch, workspace_id, expected_header):
    captured = {}

    def post(url, json=None, headers=None, auth=None, timeout=None):
        captured["headers"] = headers
        return _ack()

    monkeypatch.setattr(client.requests, "post", post)

    assert client.send("https://h", {}, workspace_id=workspace_id) is True
    if expected_header is None:
        assert "x-databricks-org-id" not in captured["headers"]
    else:
        assert captured["headers"]["x-databricks-org-id"] == expected_header
