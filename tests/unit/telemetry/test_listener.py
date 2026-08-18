from types import SimpleNamespace
from unittest.mock import Mock

from dbt.adapters.databricks.telemetry import listener


def _message(name, data):
    return SimpleNamespace(info=SimpleNamespace(name=name), data=data)


def test_end_run_records_authoritative_results_then_finalizes(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    from dbt.adapters.databricks.telemetry import hooks

    finalize = Mock()
    monkeypatch.setattr(hooks, "on_end_run_result", finalize)
    results = [SimpleNamespace(status="success")]

    listener._on_event(_message("EndRunResult", SimpleNamespace(results=results, success=True)))

    coord.record_end_run.assert_called_once_with("inv-1", ["success"], success=True)
    finalize.assert_called_once_with("inv-1")


def test_concurrency_line_records_expected_result_count(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    listener._on_event(_message("ConcurrencyLine", SimpleNamespace(node_count=7)))

    coord.record_expected_count.assert_called_once_with("inv-1", 7)


def test_hook_end_records_auxiliary_result(monkeypatch):
    coord = Mock()
    monkeypatch.setattr(listener, "coordinator", lambda: coord)
    monkeypatch.setattr(listener, "_current_invocation_id", lambda: "inv-1")

    listener._on_event(_message("LogHookEndLine", SimpleNamespace(status="success")))

    coord.record_hook_result.assert_called_once_with("inv-1", "success")


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
