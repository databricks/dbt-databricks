import json
import threading

from dbt.adapters.databricks.telemetry import coordinator as coord_mod
from dbt.adapters.databricks.telemetry import models


def _log(invocation_id="inv-1"):
    return models.TelemetryLog(
        invocation_id=invocation_id,
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        post_parse=models.PostParsePayload(
            invocation_config=models.InvocationConfig(),
            manifest_stats=models.ManifestStats(),
            connection_config=models.ConnectionConfig(),
            project_config=models.ProjectConfig(),
        ),
    )


def _run_log(invocation_id="inv-1"):
    return models.TelemetryLog(
        invocation_id=invocation_id,
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        event_type=models.EventType.POST_RUN,
        post_run=models.PostRunPayload(),
    )


def _transport(header_factory=lambda: {"Authorization": "Bearer x"}, workspace_id="42"):
    return coord_mod.Transport(
        host="https://h", header_factory=header_factory, workspace_id=workspace_id
    )


class _Capture:
    def __init__(self):
        self.calls = []

    def __call__(self, host, body, header_factory=None, workspace_id=None):
        self.calls.append((host, body, header_factory, workspace_id))
        return True


class TestTransportOpacity:
    def test_repr_is_redacted(self):
        t = _transport()
        assert "redacted" in repr(t)
        assert "Bearer" not in repr(t)

    def test_not_a_dataclass_and_no_dict(self):
        t = _transport()
        assert not hasattr(t, "__dict__")


class TestSendOrdering:
    def test_parse_then_transport_sends_once(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        assert capture.calls == []
        c.set_transport("inv-1", _transport())
        c.flush()
        assert len(capture.calls) == 1

    def test_transport_then_parse_sends_once(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        assert capture.calls == []
        c.set_post_parse("inv-1", _log())
        c.flush()
        assert len(capture.calls) == 1

    def test_repeated_parse_is_idempotent(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        c.set_post_parse("inv-1", _log())
        c.set_post_parse("inv-1", _log())
        c.flush()
        assert len(capture.calls) == 1

    def test_transport_without_auth_headers_does_not_send(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport(header_factory=None))
        c.flush()
        assert capture.calls == []

    def test_transport_does_not_block_on_slow_send(self, monkeypatch):
        in_send = threading.Event()
        release = threading.Event()

        def slow_send(host, body, header_factory=None, workspace_id=None):
            in_send.set()
            assert release.wait(timeout=2)
            return True

        monkeypatch.setattr(coord_mod.client, "send", slow_send)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        assert in_send.wait(timeout=2)
        release.set()
        c.flush(timeout=2)


class TestIsolationAndClose:
    def test_distinct_invocations_do_not_cross_pair(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-A", _log("inv-A"))
        c.set_transport("inv-B", _transport())
        c.flush()
        assert capture.calls == []

    def test_closed_invocation_rejects_late_callbacks(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.close("inv-1")
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        c.flush()
        assert capture.calls == []


class TestPostRun:
    def _entry(self, call):
        return json.loads(call[1]["protoLogs"][0])["entry"]["dbt_databricks_telemetry_log"]

    def test_both_phases_send_post_parse_first(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_post_run("inv-1", _run_log())
        c.set_transport("inv-1", _transport())
        c.flush()
        assert len(capture.calls) == 2
        assert self._entry(capture.calls[0])["event_type"] == "POST_PARSE"
        assert self._entry(capture.calls[1])["event_type"] == "POST_RUN"

    def test_post_run_waits_for_in_flight_post_parse(self, monkeypatch):
        started = threading.Event()
        release = threading.Event()
        phases = []

        def blocking_send(host, body, header_factory=None, workspace_id=None):
            entry = json.loads(body["protoLogs"][0])["entry"]["dbt_databricks_telemetry_log"]
            phases.append(entry["event_type"])
            if entry["event_type"] == "POST_PARSE":
                started.set()
                assert release.wait(timeout=2)
            return True

        monkeypatch.setattr(coord_mod.client, "send", blocking_send)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        assert started.wait(timeout=2)

        c.set_post_run("inv-1", _run_log())
        assert phases == ["POST_PARSE"]
        release.set()
        c.flush(timeout=2)

        assert phases == ["POST_PARSE", "POST_RUN"]

    def test_phases_use_distinct_event_ids(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        c.set_post_parse("inv-1", _log())
        c.set_post_run("inv-1", _run_log())
        c.flush()
        ids = {
            json.loads(call[1]["protoLogs"][0])["frontend_log_event_id"] for call in capture.calls
        }
        assert len(ids) == 2


class TestResultCapture:
    def test_node_results_fallback_snapshot(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 2)
        c.record_node_result("inv-1", "model.p.m1", "success")
        c.record_node_result("inv-1", "test.p.t", "pass")
        results, selected, expected, coverage_complete, captured = c.result_snapshot("inv-1")
        assert captured is False
        assert selected == 2
        assert expected == 2
        assert coverage_complete is False
        assert results == [("model.p.m1", "success"), ("test.p.t", "pass")]

    def test_partial_snapshot_omits_unprovable_selected_ephemeral_count(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 1)
        c.record_ephemeral_ids("inv-1", {"model.p.ephemeral1", "model.p.ephemeral2"})
        c.record_node_result("inv-1", "model.p.ephemeral1", "success")

        _, selected, _, _, captured = c.result_snapshot("inv-1")

        assert selected is None
        assert captured is False

    def test_end_run_is_authoritative_and_complete(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 2)
        c.record_node_result("inv-1", "model.p.m1", "success")
        c.record_end_run("inv-1", ["success", "skipped"])
        results, _, _, coverage_complete, _ = c.result_snapshot("inv-1")
        assert coverage_complete is True
        assert results == [("model.p.m1", "success"), (None, "skipped")]

    def test_hooks_are_auxiliary_and_subtracted_from_end_statuses(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 1)
        c.record_node_result("inv-1", "model.p.m1", "success")
        c.record_hook_result("inv-1", "success")
        c.record_end_run("inv-1", ["success", "success"])

        results, selected, expected, coverage_complete, _ = c.result_snapshot("inv-1")

        assert results == [("model.p.m1", "success"), ("operation", "success")]
        assert selected == expected == 1
        assert coverage_complete is True

    def test_fail_fast_synthetic_ephemeral_counts_as_selected_not_result(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 2)
        c.record_ephemeral_ids("inv-1", {"model.p.ephemeral"})
        c.record_node_result("inv-1", "model.p.m1", "error")
        c.record_end_run("inv-1", ["error", "skipped", "skipped"])

        results, selected, expected, coverage_complete, _ = c.result_snapshot("inv-1")

        assert results == [("model.p.m1", "error"), (None, "skipped")]
        assert selected == 3
        assert expected == 2
        assert coverage_complete is True

    def test_record_after_close_ignored(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.close("inv-1")
        c.record_node_result("inv-1", "model.p.m1", "success")
        results, _, _, _, _ = c.result_snapshot("inv-1")
        assert results == []

    def test_ephemeral_is_selected_but_not_a_top_level_result(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 1)
        c.record_ephemeral_ids("inv-1", {"model.p.ephemeral"})
        c.record_node_result("inv-1", "model.p.ephemeral", "success")
        c.record_node_result("inv-1", "model.p.table", "success")
        results, selected, expected, coverage_complete, captured = c.result_snapshot("inv-1")
        assert results == [("model.p.table", "success")]
        assert selected == 2
        assert expected == 1
        assert coverage_complete is False
        assert captured is False
