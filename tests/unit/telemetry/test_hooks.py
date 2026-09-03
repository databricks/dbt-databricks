import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.telemetry import coordinator as coord_mod
from dbt.adapters.databricks.telemetry import hooks, models
from dbt.adapters.databricks.telemetry.coordinator import Coordinator, Transport


def _parse_log():
    return models.TelemetryLog(
        invocation_id="inv-1",
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        post_parse=models.PostParsePayload(
            invocation_config=models.InvocationConfig(),
            manifest_stats=models.ManifestStats(),
            connection_config=models.ConnectionConfig(),
            project_config=models.ProjectConfig(),
        ),
    )


def _coordinator_with_retained_state() -> Coordinator:
    coord = Coordinator()
    coord.mark_start("inv-1")
    coord.set_post_parse("inv-1", Mock())
    coord.set_transport("inv-1", Transport("https://example.test", None, "42"))
    return coord


def _enable_hooks(monkeypatch, coord=None):
    if coord is None:
        coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    return coord


def test_opt_out_skips_parse_and_transport(monkeypatch):
    coord = Mock()
    build = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: False)
    monkeypatch.setattr(hooks, "DatabricksCredentials", object)
    monkeypatch.setattr(hooks.builder, "build_post_parse_log", build)
    adapter = SimpleNamespace(config=SimpleNamespace(credentials=SimpleNamespace()))

    hooks.on_post_parse(adapter, SimpleNamespace())
    hooks.on_connection_open(SimpleNamespace(), SimpleNamespace(), "/sql/1.0/warehouses/x")

    build.assert_not_called()
    coord.set_post_parse.assert_not_called()
    coord.set_transport.assert_not_called()


def test_hook_exceptions_do_not_escape(monkeypatch):
    _enable_hooks(monkeypatch)
    monkeypatch.setattr(
        hooks.builder, "build_post_parse_log", Mock(side_effect=RuntimeError("boom"))
    )
    adapter = SimpleNamespace(
        config=SimpleNamespace(credentials=SimpleNamespace()),
        get_behavior_flag_no_warn=lambda _: False,
    )
    hooks.on_post_parse(adapter, SimpleNamespace())  # must not raise


def test_run_end_exception_finalizes_stored_invocation_not_current_global(monkeypatch):
    coord = _enable_hooks(monkeypatch)
    coord.is_active.return_value = True
    coord.result_snapshot.return_value = ([], 0, 0, False, False)
    coord.outcome_snapshot.return_value = (None, False)
    coord.elapsed_ms.return_value = 1
    build = Mock(return_value="log")
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-2")
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
    coord = Coordinator()
    logs = []
    original = coord.set_post_run

    def capture(invocation_id, payload):
        logs.append(payload)
        return original(invocation_id, payload)

    coord.set_post_run = capture
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    coord.mark_start("inv-1")
    coord.record_end_run("inv-1", ["success"], success=True)

    hooks.on_command_completed("inv-1", False, 9.5)

    outcome = logs[0].post_run.run_outcome
    assert outcome.invocation_status == models.InvocationStatus.HANDLED_ERROR
    assert outcome.invocation_duration_ms == 9500
    assert coord.is_closed("inv-1") is True


def test_finalize_does_not_wait_for_send(monkeypatch):
    coord = Coordinator()
    in_send = threading.Event()
    release = threading.Event()

    def slow_send(host, body, header_factory=None, workspace_id=None):
        in_send.set()
        assert release.wait(timeout=2)
        return True

    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(coord_mod.client, "send", slow_send)
    coord.mark_start("inv-1")
    coord.set_transport(
        "inv-1",
        Transport("https://h", lambda: {"Authorization": "Bearer x"}, "42"),
    )

    hooks._finalize_post_run("inv-1", None, elapsed_ms=1, command_success=True)

    assert in_send.wait(timeout=2)
    assert coord.is_closed("inv-1") is True
    assert not release.is_set()
    release.set()
    assert coord.flush(timeout=2) is True


def test_command_completed_warns_when_flush_times_out(monkeypatch):
    coord = Coordinator()
    in_send = threading.Event()
    release = threading.Event()
    log = Mock()

    def slow_send(host, body, header_factory=None, workspace_id=None):
        in_send.set()
        assert release.wait(timeout=2)
        return True

    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(coord_mod.client, "send", slow_send)
    monkeypatch.setattr(coord_mod.client, "_TIMEOUT_SECONDS", 0)
    coord.mark_start("inv-1")
    coord.set_post_parse("inv-1", _parse_log())
    coord.set_transport(
        "inv-1",
        Transport("https://h", lambda: {"Authorization": "Bearer x"}, "42"),
    )
    assert in_send.wait(timeout=2)

    hooks.on_command_completed("inv-1", True, 1.0)

    log.warning.assert_called_once()
    assert "timed out" in log.warning.call_args.args[0].lower()
    assert coord.is_closed("inv-1") is True
    release.set()
    coord.flush(timeout=2)


def test_command_completed_cleanup_never_escapes(monkeypatch):
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

    flush.assert_called_once_with()
    assert coord.is_closed("inv-1") is True
    assert coord.is_active("inv-1") is False


def test_kernel_u2m_warns_when_telemetry_enabled(monkeypatch):
    coord = _enable_hooks(monkeypatch)
    log = Mock()
    monkeypatch.setattr(hooks, "logger", log)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: False)
    monkeypatch.setattr(hooks.listener, "register", lambda: True)
    adapter = SimpleNamespace(config=SimpleNamespace(credentials=object()))

    hooks.on_adapter_init(adapter)

    log.warning.assert_called_once()
    assert "kernel" in log.warning.call_args.args[0].lower()
    coord.mark_start.assert_called_once_with("inv-1")


@pytest.mark.parametrize(
    "http_path, manager_id, expected",
    [
        ("/sql/1.0/warehouses/named?o=42", None, "42"),
        ("/sql/1.0/warehouses/default", "7", "7"),
    ],
)
def test_connection_open_workspace_id(monkeypatch, http_path, manager_id, expected):
    coord = Mock()
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(hooks, "is_enabled_for_invocation", lambda _: True)
    monkeypatch.setattr(hooks, "has_reusable_transport", lambda _: True)
    manager = SimpleNamespace(host="https://h", header_factory=lambda: {}, workspace_id=manager_id)

    hooks.on_connection_open(SimpleNamespace(), manager, http_path)

    assert coord.set_transport.call_args.args[1].workspace_id == expected
