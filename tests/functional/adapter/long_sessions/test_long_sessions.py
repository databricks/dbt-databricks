import pytest
import os
from unittest import mock
from dbt.tests import util
from tests.functional.adapter.long_sessions import fixtures

with mock.patch.dict(
    os.environ,
    {"DBT_DATABRICKS_LONG_SESSIONS": "true", "DBT_DATABRICKS_CONNECTOR_LOG_LEVEL": "DEBUG"},
):
    import dbt.adapters.databricks.connections  # noqa


class TestLongSessionsBase:
    args_formatter = ""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": fixtures.source,
        }

    @pytest.fixture(scope="class")
    def models(self):
        m = {}
        for i in range(5):
            m[f"target{i}.sql"] = fixtures.target

        return m

    def test_long_sessions(self, project):
        _, log = util.run_dbt_and_capture(["--debug", "seed"])
        open_count = log.count("Sending request: OpenSession") / 2
        assert open_count == 2

        _, log = util.run_dbt_and_capture(["--debug", "run"])
        open_count = log.count("Sending request: OpenSession") / 2
        assert open_count == 2


class TestLongSessionsMultipleThreads(TestLongSessionsBase):
    def test_long_sessions(self, project):
        util.run_dbt_and_capture(["seed"])

        for n_threads in [1, 2, 3]:
            _, log = util.run_dbt_and_capture(["--debug", "run", "--threads", f"{n_threads}"])
            open_count = log.count("Sending request: OpenSession") / 2
            assert open_count == (n_threads + 1)


@pytest.mark.skip("May fail non-deterministically due to issue in dbt test framework.")
class TestLongSessionsMultipleCompute:
    args_formatter = ""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": fixtures.source,
        }

    @pytest.fixture(scope="class")
    def models(self):
        m = {}
        for i in range(2):
            m[f"target{i}.sql"] = fixtures.target

        m["target_alt.sql"] = fixtures.target2

        return m

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["compute"] = {
            "alternate_warehouse": {"http_path": dbt_profile_target["http_path"]},
        }

        return {"test": {"outputs": outputs, "target": "default"}}

    def test_long_sessions(self, project):
        util.run_dbt_and_capture(["--debug", "seed"])

        _, log = util.run_dbt_and_capture(["--debug", "run"])
        open_count = log.count("Sending request: OpenSession") / 2
        assert open_count == 3


@pytest.mark.skip("May fail non-deterministically due to issue in dbt test framework.")
class TestLongSessionsIdleCleanup(TestLongSessionsMultipleCompute):
    args_formatter = ""

    @pytest.fixture(scope="class")
    def models(self):
        m = {
            "targetseq1.sql": fixtures.targetseq1,
            "targetseq2.sql": fixtures.targetseq2,
            "targetseq3.sql": fixtures.targetseq3,
            "targetseq4.sql": fixtures.targetseq4,
            "targetseq5.sql": fixtures.targetseq5,
        }
        return m

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["connect_max_idle"] = 1
        outputs["default"]["compute"] = {
            "alternate_warehouse": {
                "http_path": dbt_profile_target["http_path"],
                "connect_max_idle": 1,
            },
        }

        return {"test": {"outputs": outputs, "target": "default"}}

    def test_long_sessions(self, project):
        util.run_dbt(["--debug", "seed"])

        _, log = util.run_dbt_and_capture(["--debug", "run"])
        idle_count = log.count("closing idle connection") / 2
        assert idle_count > 0
