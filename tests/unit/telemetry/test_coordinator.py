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

    def test_timestamp_millis_is_parse_time_not_send_time(self, monkeypatch):
        clock = {"now": 10.0}
        monkeypatch.setattr(coord_mod.time, "time", lambda: clock["now"])
        monkeypatch.setattr(coord_mod.encoder.time, "time", lambda: clock["now"])
        capture = _Capture()
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        clock["now"] = 40.0
        c.set_transport("inv-1", _transport())
        c.flush()
        body = capture.calls[0][1]
        fe = json.loads(body["protoLogs"][0])
        assert fe["context"]["client_context"]["timestamp_millis"] == 10_000
        assert body["uploadTime"] == 40_000


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

    def test_close_does_not_cancel_already_queued_send(self, monkeypatch):
        pending = []

        class DelayedThread:
            def __init__(self, target, args, name, daemon):
                self.target = target
                self.args = args

            def is_alive(self):
                return False

            def start(self):
                pending.append(self)

        capture = _Capture()
        monkeypatch.setattr(coord_mod.threading, "Thread", DelayedThread)
        monkeypatch.setattr(coord_mod.client, "send", capture)
        c = coord_mod.Coordinator()
        c.set_post_parse("inv-1", _log())
        c.set_transport("inv-1", _transport())

        assert len(pending) == 1
        c.close("inv-1")
        pending[0].target(*pending[0].args)

        assert len(capture.calls) == 1

    def test_mark_start_prunes_closed_invocations(self):
        c = coord_mod.Coordinator()
        c.mark_start("inv-1")
        c.close("inv-1")
        c.mark_start("inv-2")

        assert "inv-1" not in c._states
        assert "inv-2" in c._states
        assert c.needs_post_parse("inv-2") is True

        c.close("inv-1")
        assert c._states["inv-1"].closed
        assert not c._states["inv-2"].closed
