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
            result = probe.probe_host("peco.azuredatabricks.net")
        assert result == HostMetadata(host_type="unified", account_id="acct-123")
        m.assert_called_once_with(
            "https://peco.azuredatabricks.net/.well-known/databricks-config",
            timeout=5,
        )

    def test_legacy_host_serves_endpoint_but_not_unified(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "workspace"}),
        ):
            result = probe.probe_host("legacy.azuredatabricks.net")
        assert result.host_type == "workspace"

    def test_result_cached_per_host(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "unified"}),
        ) as m:
            probe.probe_host("peco.azuredatabricks.net")
            probe.probe_host("peco.azuredatabricks.net")
        assert m.call_count == 1


class TestProbeHostRetry:
    def test_retries_three_times_on_failure(self):
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                side_effect=requests.ConnectionError("network down"),
            ) as m,
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep") as sleep_mock,
            mock.patch("dbt.adapters.databricks.spog.probe.logger") as mock_logger,
        ):
            result = probe.probe_host("flaky.example.com")
        assert m.call_count == 3
        assert sleep_mock.call_count == 2  # sleep between attempts 1->2 and 2->3
        assert result == HostMetadata(host_type=None)
        # WARN must be emitted and mention the host
        mock_logger.warning.assert_called_once()
        warn_msg = mock_logger.warning.call_args[0][0]
        assert "flaky.example.com" in warn_msg
        assert "3 attempts" in warn_msg

    def test_recovers_on_second_attempt(self):
        responses = [
            requests.ConnectionError("transient"),
            _mock_response({"host_type": "unified", "account_id": "acct-x"}),
        ]
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                side_effect=responses,
            ) as m,
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep"),
        ):
            result = probe.probe_host("recovers.example.com")
        assert m.call_count == 2
        assert result.host_type == "unified"

    def test_http_error_retries_then_falls_back(self):
        """Non-200 responses (404, 500, 503, etc.) trigger raise_for_status,
        which the retry loop catches via requests.RequestException — same
        treatment as a network failure: three attempts, then fall back."""
        bad_resp = mock.Mock(spec=requests.Response)
        bad_resp.status_code = 503
        bad_resp.raise_for_status = mock.Mock(
            side_effect=requests.HTTPError("503 Service Unavailable")
        )
        with (
            mock.patch(
                "dbt.adapters.databricks.spog.probe.requests.get",
                return_value=bad_resp,
            ) as m,
            mock.patch("dbt.adapters.databricks.spog.probe.time.sleep") as sleep_mock,
            mock.patch("dbt.adapters.databricks.spog.probe.logger") as mock_logger,
        ):
            result = probe.probe_host("svc-down.example.com")
        assert m.call_count == 3
        assert sleep_mock.call_count == 2
        assert result == HostMetadata(host_type=None)
        mock_logger.warning.assert_called_once()
        warn_msg = mock_logger.warning.call_args[0][0]
        assert "svc-down.example.com" in warn_msg

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
