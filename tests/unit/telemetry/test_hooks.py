from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import hooks


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


def test_closed_invocation_is_not_finalized_again(monkeypatch):
    coord = Mock()
    coord.is_closed.return_value = True
    build = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks.builder, "build_post_run_log", build)

    hooks._finalize_post_run("inv-1", None)

    coord.is_closed.assert_called_once_with("inv-1")
    coord.result_snapshot.assert_not_called()
    build.assert_not_called()


def test_finalize_does_not_wait_for_send(monkeypatch):
    coord = Mock()
    coord.is_closed.return_value = False
    coord.result_snapshot.return_value = ([], 0, 0, False, False)
    coord.outcome_snapshot.return_value = (None, False)
    coord.elapsed_ms.return_value = 1
    build = Mock(return_value="log")
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks.builder, "build_post_run_log", build)

    hooks._finalize_post_run("inv-1", None)

    coord.set_post_run.assert_called_once_with("inv-1", "log")
    coord.flush.assert_not_called()
    coord.close.assert_called_once_with("inv-1")


def test_run_end_exception_finalizes_stored_invocation_not_current_global(monkeypatch):
    coord = Mock()
    coord.is_closed.return_value = False
    coord.result_snapshot.return_value = ([], 0, 0, False, False)
    coord.outcome_snapshot.return_value = (None, False)
    coord.elapsed_ms.return_value = 1
    build = Mock(return_value="log")
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-2")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks.builder, "build_post_run_log", build)
    monkeypatch.setattr(hooks.sys, "exc_info", lambda: (RuntimeError, RuntimeError("boom"), None))
    adapter = SimpleNamespace(
        config=SimpleNamespace(credentials=SimpleNamespace()),
        _dbt_telemetry_invocation_id="inv-1",
    )

    hooks.on_run_end(adapter)

    build.assert_called_once()
    assert build.call_args.args[0] == "inv-1"
    coord.set_post_run.assert_called_once_with("inv-1", "log")
    coord.close.assert_called_once_with("inv-1")


def test_connection_open_uses_opened_path_workspace_id(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
    manager = SimpleNamespace(host="https://h", header_factory=lambda: {}, workspace_id=None)

    hooks.on_connection_open(
        SimpleNamespace(), manager, "/sql/1.0/warehouses/named?o=42"
    )

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
