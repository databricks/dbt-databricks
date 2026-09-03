from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.telemetry import hooks, listener, models
from dbt.adapters.databricks.telemetry.coordinator import Coordinator


def _message(name, data):
    return SimpleNamespace(info=SimpleNamespace(name=name), data=data)


@pytest.mark.parametrize("started", [True, False], ids=["active", "opt_out"])
def test_end_run_routing(monkeypatch, started):
    coord = Coordinator()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")
    if started:
        coord.mark_start("inv-1")
        coord.record_expected_count("inv-1", 1)

    listener._on_event(
        _message(
            "EndRunResult",
            SimpleNamespace(results=[SimpleNamespace(status="success")], success=True),
        )
    )

    results, _, _, coverage_complete, captured = coord.result_snapshot("inv-1")
    if started:
        assert coord.is_active("inv-1") is True
        assert captured is True
        assert coverage_complete is True
        assert results == [(None, "success")]
    else:
        assert coord.is_active("inv-1") is False
        assert captured is False
        assert results == []


@pytest.mark.parametrize("already_closed", [False, True], ids=["active", "already_closed"])
def test_command_completed_routes_to_hook(monkeypatch, already_closed):
    coord = Coordinator()
    logs = []
    original = coord.set_post_run

    def capture(invocation_id, payload):
        logs.append(payload)
        return original(invocation_id, payload)

    coord.set_post_run = capture
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(hooks, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")
    coord.mark_start("inv-1")
    if already_closed:
        coord.close("inv-1")

    listener._on_event(_message("CommandCompleted", SimpleNamespace(success=True, elapsed=1.0)))

    if already_closed:
        assert logs == []
    else:
        assert len(logs) == 1
        assert logs[0].event_type == models.EventType.POST_RUN
    assert coord.is_closed("inv-1") is True


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
