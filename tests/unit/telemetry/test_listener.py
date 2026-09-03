from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import listener
from dbt.adapters.databricks.telemetry.coordinator import Coordinator


def _message(name, data):
    return SimpleNamespace(info=SimpleNamespace(name=name), data=data)


def test_end_run_records_without_closing(monkeypatch):
    coord = Coordinator()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")
    coord.mark_start("inv-1")
    coord.record_expected_count("inv-1", 1)

    listener._on_event(
        _message(
            "EndRunResult",
            SimpleNamespace(results=[SimpleNamespace(status="success")], success=True),
        )
    )

    assert coord.is_active("inv-1") is True
    results, _, _, coverage_complete, captured = coord.result_snapshot("inv-1")
    assert captured is True
    assert coverage_complete is True
    assert results == [(None, "success")]


def test_command_completed_reaches_hook_after_early_close(monkeypatch):
    coord = Mock()
    coord.is_active.return_value = False
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

    assert coord.is_active("disabled-1") is False
    results, selected, expected, coverage_complete, captured = coord.result_snapshot("disabled-1")
    assert results == []
    assert selected == 0
    assert expected == 0
    assert coverage_complete is False
    assert captured is False


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
