from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug
from dbt.tests import util


class TestDatabricksDebug(BaseDebug):
    def test_ok(self, project):
        util.run_dbt(["debug"])
        stdout = self.capsys.readouterr().out
        assert "ERROR" not in stdout
        assert "host:" in stdout
        assert "http_path:" in stdout
        assert "schema:" in stdout
        assert "catalog:" in stdout
