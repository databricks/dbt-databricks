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
