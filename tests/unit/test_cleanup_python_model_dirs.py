"""Unit tests for scripts/cleanup_python_model_dirs.py.

The script lives in scripts/ (not the importable package), so load it by path.
"""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

from databricks.sdk.errors import ResourceDoesNotExist

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cleanup_python_model_dirs.py"
_spec = importlib.util.spec_from_file_location("cleanup_python_model_dirs", _SCRIPT)
assert _spec is not None and _spec.loader is not None
cleanup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cleanup)


class TestNormalizeHost:
    def test_bare_hostname_gets_https(self):
        assert cleanup.normalize_host("adb-123.4.azuredatabricks.net") == (
            "https://adb-123.4.azuredatabricks.net"
        )

    def test_full_url_unchanged(self):
        assert cleanup.normalize_host("https://adb-123.4.azuredatabricks.net") == (
            "https://adb-123.4.azuredatabricks.net"
        )


class TestPythonModelsRoot:
    def test_builds_user_scoped_path(self):
        # Mirrors UserFolderApi.get_folder's /Users/{user}/dbt_python_models prefix.
        assert (
            cleanup.python_models_root("c2108ab0-1c66-4fa0-ae95-3baa5e160017")
            == "/Users/c2108ab0-1c66-4fa0-ae95-3baa5e160017/dbt_python_models"
        )


def _client(user_name="sp@example.com"):
    client = Mock()
    client.current_user.me.return_value.user_name = user_name
    return client


class TestPurge:
    def test_deletes_whole_tree_recursively(self):
        client = _client("me@example.com")
        cleanup.purge(client)
        client.workspace.delete.assert_called_once_with(
            "/Users/me@example.com/dbt_python_models", recursive=True
        )

    def test_missing_folder_is_a_noop(self):
        client = _client()
        client.workspace.delete.side_effect = ResourceDoesNotExist("not found")
        # Must not raise — a not-yet-created folder is expected on first run.
        cleanup.purge(client)

    def test_dry_run_lists_and_does_not_delete(self):
        client = _client()
        client.workspace.list.return_value = [Mock(), Mock(), Mock()]
        cleanup.purge(client, dry_run=True)
        client.workspace.list.assert_called_once_with(
            f"/Users/{client.current_user.me.return_value.user_name}/dbt_python_models"
        )
        client.workspace.delete.assert_not_called()

    def test_dry_run_missing_folder_is_a_noop(self):
        client = _client()
        client.workspace.list.side_effect = ResourceDoesNotExist("not found")
        # Must not raise — folder may not exist yet on a first run.
        cleanup.purge(client, dry_run=True)
        client.workspace.delete.assert_not_called()


class TestMain:
    def test_returns_zero_and_annotates_on_failure(self, monkeypatch, capsys):
        # Best-effort contract: any failure is surfaced loudly but never blocks.
        monkeypatch.setattr(cleanup, "build_client", Mock(side_effect=RuntimeError("auth boom")))
        monkeypatch.setattr("sys.argv", ["cleanup_python_model_dirs.py"])
        assert cleanup.main() == 0
        out = capsys.readouterr().out
        assert "::error::" in out
        assert "auth boom" in out

    def test_returns_zero_on_success(self, monkeypatch):
        monkeypatch.setattr(cleanup, "build_client", Mock(return_value=_client()))
        monkeypatch.setattr("sys.argv", ["cleanup_python_model_dirs.py"])
        assert cleanup.main() == 0
