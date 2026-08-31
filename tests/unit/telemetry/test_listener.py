from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import listener
from dbt.adapters.databricks.telemetry.coordinator import Coordinator


def _message(name, data):
    return SimpleNamespace(info=SimpleNamespace(name=name), data=data)


def test_end_run_records_without_finalizing(monkeypatch):
    coord = Mock()
    coord.is_active.return_value = True
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    from dbt.adapters.databricks.telemetry import hooks

    finalize = Mock()
    monkeypatch.setattr(hooks, "on_command_completed", finalize)
    results = [SimpleNamespace(status="success")]

    listener._on_event(_message("EndRunResult", SimpleNamespace(results=results, success=True)))

    coord.record_end_run.assert_called_once_with("inv-1", ["success"], success=True)
    finalize.assert_not_called()


def test_command_completed_finalizes(monkeypatch):
    coord = Mock()
    coord.is_active.return_value = True
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    from dbt.adapters.databricks.telemetry import hooks

    finalize = Mock()
    monkeypatch.setattr(hooks, "on_command_completed", finalize)

    listener._on_event(_message("CommandCompleted", SimpleNamespace(success=False, elapsed=9.5)))

    finalize.assert_called_once_with("inv-1", False, 9.5)


def test_opt_out_end_run_does_not_create_state(monkeypatch):
    coord = Coordinator()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "disabled-1")

    listener._on_event(
        _message(
            "EndRunResult",
            SimpleNamespace(results=[SimpleNamespace(status="success")], success=True),
        )
    )

    assert coord._states == {}


def test_repeated_opt_out_end_runs_do_not_accumulate_states(monkeypatch):
    coord = Coordinator()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    ids = [f"disabled-{i}" for i in range(5)]
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: ids.pop(0) if ids else "")

    msg = _message(
        "EndRunResult",
        SimpleNamespace(results=[SimpleNamespace(status="success")], success=True),
    )
    for _ in range(5):
        listener._on_event(msg)

    assert coord._states == {}


def test_generic_exception_records_typed_error(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    listener._on_event(
        _message(
            "GenericExceptionOnRun",
            SimpleNamespace(
                unique_id="model.p.m",
                node_info=SimpleNamespace(unique_id="model.p.m"),
            ),
        )
    )

    coord.record_node_result.assert_called_once_with("inv-1", "model.p.m", "error")


def test_pre_end_failure_marks_fail_fast_triggered(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")
    monkeypatch.setattr(listener, "_fail_fast_enabled", lambda: True)

    listener._on_event(_message("RunResultFailure", SimpleNamespace()))

    coord.mark_fail_fast_triggered.assert_called_once_with("inv-1")
