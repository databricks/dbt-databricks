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
