from types import SimpleNamespace

import pytest

from dbt.adapters.databricks.telemetry import client


def _ack():
    return SimpleNamespace(status_code=200, json=lambda: {"numProtoSuccess": 1, "errors": None})


@pytest.mark.parametrize(
    "workspace_id, expected",
    [
        pytest.param("42", "42", id="numeric"),
        pytest.param("customer-name", None, id="non_numeric"),
    ],
)
def test_workspace_id_header(monkeypatch, workspace_id, expected):
    captured = {}

    def post(url, json=None, headers=None, auth=None, timeout=None):
        captured["headers"] = headers
        return _ack()

    monkeypatch.setattr(client.requests, "post", post)

    assert client.send("https://h", {}, workspace_id=workspace_id) is True
    if expected is None:
        assert "x-databricks-org-id" not in captured["headers"]
    else:
        assert captured["headers"]["x-databricks-org-id"] == expected
