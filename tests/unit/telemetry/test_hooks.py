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
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
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
    assert "inv-1" not in coord._states


def test_kernel_u2m_warns_when_telemetry_enabled(monkeypatch):
    coord = Mock()
    log = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: False)
    adapter = SimpleNamespace(config=SimpleNamespace(credentials=object()))

    hooks.on_adapter_init(adapter)

    log.warning.assert_called_once()
    assert "kernel" in log.warning.call_args.args[0].lower()
    coord.mark_start.assert_called_once_with("inv-1")


def test_reusable_transport_does_not_warn_on_init(monkeypatch):
    coord = Mock()
    log = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
    adapter = SimpleNamespace(config=SimpleNamespace(credentials=object()))

    hooks.on_adapter_init(adapter)

    log.warning.assert_not_called()
    coord.mark_start.assert_called_once_with("inv-1")


def test_connection_open_uses_opened_path_workspace_id(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
    manager = SimpleNamespace(host="https://h", header_factory=lambda: {}, workspace_id=None)

    hooks.on_connection_open(SimpleNamespace(), manager, "/sql/1.0/warehouses/named?o=42")

    transport = coord.set_transport.call_args.args[1]
    assert transport.workspace_id == "42"


def test_connection_open_falls_back_to_manager_workspace_id(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
    manager = SimpleNamespace(host="https://h", header_factory=lambda: {}, workspace_id="7")

    hooks.on_connection_open(SimpleNamespace(), manager, "/sql/1.0/warehouses/default")

    transport = coord.set_transport.call_args.args[1]
    assert transport.workspace_id == "7"
