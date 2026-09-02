from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import hooks
from dbt.adapters.databricks.telemetry.coordinator import Coordinator, Transport


def _coordinator_with_retained_state() -> Coordinator:
    coord = Coordinator()
    coord.mark_start("inv-1")
    coord.set_post_parse("inv-1", Mock())
    coord.set_transport("inv-1", Transport("https://example.test", None, "42"))
    return coord


def _assert_invocation_closed_and_scrubbed(coord: Coordinator) -> None:
    state = coord._states["inv-1"]
    assert state.closed is True
    assert state.post_parse is None
    assert state.transport is None


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
    coord.is_active.return_value = False
    build = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks.builder, "build_post_run_log", build)

    hooks._finalize_post_run("inv-1", None)

    coord.is_active.assert_called_once_with("inv-1")
    coord.result_snapshot.assert_not_called()
    build.assert_not_called()


def test_finalize_does_not_wait_for_send(monkeypatch):
    coord = Mock()
    coord.is_active.return_value = True
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
    coord.is_active.return_value = True
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


def test_command_completed_overrides_graph_success_and_elapsed(monkeypatch):
    from dbt.adapters.databricks.telemetry.coordinator import Coordinator

    coord = Coordinator()
    captured = {}

    def capture(
        invocation_id,
        elapsed_ms,
        exc_type,
        results,
        expected,
        coverage_complete,
        results_captured,
        selected_resources=None,
        fail_fast_triggered=False,
        task_success=None,
    ):
        captured["elapsed_ms"] = elapsed_ms
        captured["task_success"] = task_success
        return SimpleNamespace()

    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks.builder, "build_post_run_log", capture)
    coord.mark_start("inv-1")
    coord.record_end_run("inv-1", ["success"], success=True)

    hooks.on_command_completed("inv-1", False, 9.5)

    assert captured["task_success"] is False
    assert captured["elapsed_ms"] == 9500
    assert coord.is_closed("inv-1") is True


def test_command_completed_waits_for_all_telemetry_senders(monkeypatch):
    coord = Mock()
    log = Mock()
    order = []

    def finalize(*args, **kwargs):
        order.append("finalize")

    def flush():
        order.append("flush")
        return True

    coord.flush.side_effect = flush
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "_finalize_post_run", finalize)

    hooks.on_command_completed("inv-1", True, 1.0)

    assert order == ["finalize", "flush"]
    log.warning.assert_not_called()


def test_command_completed_warns_when_flush_times_out(monkeypatch):
    coord = Mock()
    coord.flush.return_value = False
    log = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "_finalize_post_run", Mock())

    hooks.on_command_completed("inv-1", True, 1.0)

    log.warning.assert_called_once_with("Timed out waiting for dbt telemetry delivery to finish.")


def test_command_completed_still_flushes_when_finalization_fails(monkeypatch):
    coord = _coordinator_with_retained_state()
    flush = Mock(wraps=coord.flush)
    monkeypatch.setattr(coord, "flush", flush)
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(
        hooks,
        "_finalize_post_run",
        Mock(side_effect=RuntimeError("finalization failed")),
    )

    hooks.on_command_completed("inv-1", False, 1.0)

    _assert_invocation_closed_and_scrubbed(coord)
    flush.assert_called_once_with()


def test_command_completed_closes_when_elapsed_conversion_fails(monkeypatch):
    coord = _coordinator_with_retained_state()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)

    class InvalidElapsed:
        def __float__(self) -> float:
            raise ValueError("invalid elapsed")

    hooks.on_command_completed("inv-1", False, InvalidElapsed())

    _assert_invocation_closed_and_scrubbed(coord)


def test_kernel_u2m_warns_when_telemetry_enabled(monkeypatch):
    coord = Mock()
    log = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: False)
    monkeypatch.setattr(hooks.listener, "register", lambda: True)
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
    monkeypatch.setattr(hooks.listener, "register", lambda: True)
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
