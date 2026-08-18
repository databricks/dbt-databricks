import json

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
        # Opaque handle must not offer easy serialization.
        t = _transport()
        assert not hasattr(t, "__dict__")  # __slots__ only


class TestSendOrdering:
    def test_parse_then_transport_sends_once(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        assert capture.calls == []
        c.set_transport("inv-1", _transport())
        assert len(capture.calls) == 1

    def test_transport_then_parse_sends_once(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        assert capture.calls == []
        c.set_post_parse("inv-1", _log())
        assert len(capture.calls) == 1

    def test_repeated_parse_is_idempotent(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        c.set_post_parse("inv-1", _log())
        c.set_post_parse("inv-1", _log())
        assert len(capture.calls) == 1

    def test_transport_without_auth_headers_does_not_send(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport(header_factory=None))
        assert capture.calls == []

    def test_stable_event_id_in_payload(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        _, body, _, _ = capture.calls[0]
        fe = json.loads(body["protoLogs"][0])
        assert fe["frontend_log_event_id"]  # a stable UUID was assigned


class TestIsolationAndClose:
    def test_distinct_invocations_do_not_cross_pair(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-A", _log("inv-A"))
        c.set_transport("inv-B", _transport())
        # A has no transport, B has no payload -> nothing sends.
        assert capture.calls == []

    def test_closed_invocation_rejects_late_callbacks(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.close("inv-1")
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())
        assert capture.calls == []


class TestPostRun:
    def _entry(self, call):
        return json.loads(call[1]["protoLogs"][0])["entry"]["dbt_databricks_telemetry_log"]

    def test_post_run_sends_after_transport(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_run("inv-1", _run_log())
        assert capture.calls == []
        c.set_transport("inv-1", _transport())
        assert len(capture.calls) == 1

    def test_both_phases_send_post_parse_first(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_post_run("inv-1", _run_log())
        c.set_transport("inv-1", _transport())
        assert len(capture.calls) == 2
        assert self._entry(capture.calls[0])["event_type"] == "POST_PARSE"
        assert self._entry(capture.calls[1])["event_type"] == "POST_RUN"

    def test_phases_use_distinct_event_ids(self, monkeypatch):
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_transport("inv-1", _transport())
        c.set_post_parse("inv-1", _log())
        c.set_post_run("inv-1", _run_log())
        ids = {
            json.loads(call[1]["protoLogs"][0])["frontend_log_event_id"] for call in capture.calls
        }
        assert len(ids) == 2

    def test_elapsed_ms(self):
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        assert c.elapsed_ms("inv-1") >= 0
        assert c.elapsed_ms("missing") == 0

    def test_mark_start_enables_timer(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        assert c.elapsed_ms("inv-1") >= 0


class TestResultCapture:
    def test_record_and_snapshot(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.begin_result_capture("inv-1")
        c.record_node_result("inv-1", "model", "success")
        c.record_node_result("inv-1", "test", "pass")
        node_results, captured = c.result_snapshot("inv-1")
        assert captured is True
        assert node_results == [("model", "success"), ("test", "pass")]

    def test_snapshot_missing_invocation(self):
        c = coord_mod.Coordinator()
        assert c.result_snapshot("nope") == ([], False)

    def test_record_after_close_ignored(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.close("inv-1")
        c.record_node_result("inv-1", "model", "success")
        node_results, _ = c.result_snapshot("inv-1")
        assert node_results == []
