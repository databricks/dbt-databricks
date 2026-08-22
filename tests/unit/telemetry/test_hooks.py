from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import hooks
from dbt.adapters.databricks.telemetry.coordinator import Coordinator


def test_post_parse_is_not_rebuilt(monkeypatch):
    coord = Mock()
    coord.needs_post_parse.return_value = False
    build = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks.builder, "build_post_parse_log", build)
    adapter = SimpleNamespace(config=SimpleNamespace(credentials=SimpleNamespace()))

    hooks.on_post_parse(adapter, SimpleNamespace())

    coord.needs_post_parse.assert_called_once_with("inv-1")
    build.assert_not_called()


def test_run_end_closes_without_waiting(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    adapter = SimpleNamespace(
        config=SimpleNamespace(credentials=SimpleNamespace()),
        _dbt_telemetry_invocation_id="inv-1",
    )

    hooks.on_run_end(adapter)

    coord.flush.assert_not_called()
    coord.close.assert_called_once_with("inv-1")


def test_run_end_closes_stored_invocation_not_current_global(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-2")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    adapter = SimpleNamespace(
        config=SimpleNamespace(credentials=SimpleNamespace()),
        _dbt_telemetry_invocation_id="inv-1",
    )

    hooks.on_run_end(adapter)

    coord.close.assert_called_once_with("inv-1")


def test_stale_cleanup_does_not_tombstone_next_invocation(monkeypatch):
    coord = Coordinator()
    current = ["inv-1"]
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: current[0])
    leftover = SimpleNamespace(config=SimpleNamespace(credentials=object()))

    hooks.on_adapter_init(leftover)
    hooks.on_run_end(leftover)
    current[0] = "inv-2"
    hooks.on_run_end(leftover)
    next_adapter = SimpleNamespace(config=SimpleNamespace(credentials=object()))
    hooks.on_adapter_init(next_adapter)

    assert leftover._dbt_telemetry_invocation_id == "inv-1"
    assert next_adapter._dbt_telemetry_invocation_id == "inv-2"
    assert coord.needs_post_parse("inv-2") is True
