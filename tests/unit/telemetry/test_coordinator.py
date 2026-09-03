import json
import threading

import pytest

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


def _entry(call):
    return json.loads(call[1]["protoLogs"][0])["entry"]["dbt_databricks_telemetry_log"]


class TestTransportOpacity:
    def test_repr_is_redacted(self):
        t = _transport()
        assert "redacted" in repr(t)
        assert "Bearer" not in repr(t)


class TestSendOrdering:
    @pytest.mark.parametrize("order", ["parse_then_transport", "transport_then_parse"])
    def test_parse_and_transport_send_once(self, monkeypatch, order):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        if order == "parse_then_transport":
            c.set_post_parse("inv-1", _log())
            assert capture.calls == []
            c.set_transport("inv-1", _transport())
        else:
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

    def test_flush_reports_whether_all_senders_stopped(self, monkeypatch):
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

        assert c.flush(timeout=0) is False
        release.set()
        assert c.flush(timeout=2) is True

    def test_send_exceptions_do_not_escape(self, monkeypatch):
        def boom(host, body, header_factory=None, workspace_id=None):
            raise RuntimeError("network")

        monkeypatch.setattr(coord_mod.client, "send", boom)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        assert c.flush(timeout=2) is True


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
    def test_both_phases_send_post_parse_first(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_post_run("inv-1", _run_log())
        c.set_transport("inv-1", _transport())
        c.flush()
        assert len(capture.calls) == 2
        assert _entry(capture.calls[0])["event_type"] == "POST_PARSE"
        assert _entry(capture.calls[1])["event_type"] == "POST_RUN"

    def test_close_does_not_drop_post_run_behind_in_flight_parse(self, monkeypatch):
        started = threading.Event()
        release = threading.Event()
        phases = []

        def blocking_send(host, body, header_factory=None, workspace_id=None):
            entry = _entry((host, body))
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
        c.close("inv-1")
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

    def test_timestamp_millis_is_event_time_not_send_time(self, monkeypatch):
        pending = []

        class DelayedThread:
            def __init__(self, target, args, name, daemon):
                self.target = target
                self.args = args

            def is_alive(self):
                return False

            def start(self):
                pending.append(self)

        clock = {"now": 10.0}
        monkeypatch.setattr(coord_mod.time, "time", lambda: clock["now"])
        monkeypatch.setattr(coord_mod.encoder.time, "time", lambda: clock["now"])
        monkeypatch.setattr(coord_mod.threading, "Thread", DelayedThread)
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        clock["now"] = 20.0
        c.set_post_run("inv-1", _run_log())
        clock["now"] = 40.0
        for worker in pending:
            worker.target(*worker.args)

        parse_body = capture.calls[0][1]
        run_body = capture.calls[1][1]
        parse_fe = json.loads(parse_body["protoLogs"][0])
        run_fe = json.loads(run_body["protoLogs"][0])
        assert parse_fe["context"]["client_context"]["timestamp_millis"] == 10_000
        assert run_fe["context"]["client_context"]["timestamp_millis"] == 20_000
        assert parse_body["uploadTime"] == 40_000
        assert run_body["uploadTime"] == 40_000


class TestResultCapture:
    def test_partial_snapshot_omits_unprovable_selected_ephemeral_count(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 1)
        c.record_ephemeral_ids("inv-1", {"model.p.ephemeral1", "model.p.ephemeral2"})
        c.record_node_result("inv-1", "model.p.ephemeral1", "success")

        _, selected, _, coverage_complete, captured = c.result_snapshot("inv-1")

        assert selected is None
        assert coverage_complete is False
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

    def test_observed_ephemeral_is_not_double_counted_on_fail_fast(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 2)
        c.record_ephemeral_ids("inv-1", {"model.p.ephemeral"})
        c.record_node_result("inv-1", "model.p.ephemeral", "success")
        c.record_node_result("inv-1", "model.p.failed", "error")
        c.record_end_run("inv-1", ["error", "skipped", "skipped"])

        results, selected, expected, coverage_complete, _ = c.result_snapshot("inv-1")

        assert results == [("model.p.failed", "error"), (None, "skipped")]
        assert selected == 3
        assert expected == 2
        assert coverage_complete is True

    def test_build_unit_test_raises_expected_above_concurrency_line(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.record_expected_count("inv-1", 1)
        c.record_node_result("inv-1", "unit_test.p.u1", "pass")
        c.record_node_result("inv-1", "model.p.m1", "success")
        c.record_end_run("inv-1", ["pass", "success"])

        results, selected, expected, coverage_complete, _ = c.result_snapshot("inv-1")

        assert results == [("unit_test.p.u1", "pass"), ("model.p.m1", "success")]
        assert selected == 2
        assert expected == 2
        assert coverage_complete is True

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
