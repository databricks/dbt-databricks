from types import SimpleNamespace

from dbt.adapters.databricks.telemetry import client


def _ack(**overrides):
    payload = {"numProtoSuccess": 1, "errors": None}
    payload.update(overrides)
    return SimpleNamespace(status_code=200, json=lambda: payload)


def test_numeric_workspace_id_is_sent_in_header(monkeypatch):
    captured = {}

    def post(url, json=None, headers=None, auth=None, timeout=None):
        captured["headers"] = headers
        return _ack()

    monkeypatch.setattr(client.requests, "post", post)

    assert client.send("https://h", {}, workspace_id="42") is True
    assert captured["headers"]["x-databricks-org-id"] == "42"


def test_non_numeric_workspace_id_is_omitted_from_header(monkeypatch):
    captured = {}

    def post(url, json=None, headers=None, auth=None, timeout=None):
        captured["headers"] = headers
        return _ack()

    monkeypatch.setattr(client.requests, "post", post)

    assert client.send("https://h", {}, workspace_id="customer-name") is True
    assert "x-databricks-org-id" not in captured["headers"]
