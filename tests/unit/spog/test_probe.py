from unittest import mock

import pytest
import requests

from dbt.adapters.databricks.spog import probe
from dbt.adapters.databricks.spog.probe import HostMetadata


@pytest.fixture(autouse=True)
def clear_probe_cache():
    probe.probe_host.cache_clear()
    yield
    probe.probe_host.cache_clear()


def _mock_response(json_body, status_code=200):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status = mock.Mock()
    return resp


class TestProbeHostHappyPath:
    def test_unified_host(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "unified", "account_id": "acct-123"}),
        ) as m:
            result = probe.probe_host("spog.example.com")
        assert result == HostMetadata(host_type="unified", account_id="acct-123")
        m.assert_called_once_with(
            "https://spog.example.com/.well-known/databricks-config",
            timeout=5,
        )

    def test_result_cached_per_host(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "unified"}),
        ) as m:
            probe.probe_host("spog.example.com")
            probe.probe_host("spog.example.com")
        assert m.call_count == 1


class TestProbeHostRetry:
    def test_retries_three_times_on_failure(self):
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                side_effect=requests.ConnectionError("network down"),
            ) as m,
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep"),
            mock.patch("dbt.adapters.databricks.spog.probe.logger"),
        ):
            result = probe.probe_host("flaky.example.com")
        assert m.call_count == 3
        assert result == HostMetadata(host_type=None)

    def test_json_decode_error_treated_as_failure(self):
        bad_resp = mock.Mock(spec=requests.Response)
        bad_resp.status_code = 200
        bad_resp.raise_for_status = mock.Mock()
        bad_resp.json.side_effect = ValueError("not json")
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                return_value=bad_resp,
            ),
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep"),
        ):
            result = probe.probe_host("badjson.example.com")
        assert result == HostMetadata(host_type=None)
