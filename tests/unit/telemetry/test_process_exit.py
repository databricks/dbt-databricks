import subprocess
import sys
import textwrap


def test_command_completed_waits_for_post_run_before_process_exit():
    script = textwrap.dedent(
        """
        import threading
        import time

        from dbt.adapters.databricks.telemetry import coordinator as coord_mod
        from dbt.adapters.databricks.telemetry import hooks, models

        coord = coord_mod.Coordinator()
        started = threading.Event()

        def delayed_send(host, body, header_factory=None, workspace_id=None):
            print("POST_RUN_START", flush=True)
            started.set()
            time.sleep(0.2)
            print("POST_RUN_DONE", flush=True)
            return True

        def finalize(invocation_id, exc_type, **kwargs):
            payload = models.TelemetryLog(
                invocation_id=invocation_id,
                adapter_version="1.2.3",
                dbt_core_version="1.12.0",
                event_type=models.EventType.POST_RUN,
                post_run=models.PostRunPayload(),
            )
            coord.set_post_run(invocation_id, payload)
            assert started.wait(timeout=1)
            coord.close(invocation_id)

        coord_mod.client.send = delayed_send
        hooks.coordinator = lambda: coord
        hooks._finalize_post_run = finalize
        coord.mark_start("inv-1")
        coord.set_transport(
            "inv-1",
            coord_mod.Transport(
                host="https://example.test",
                header_factory=lambda: {"Authorization": "Bearer x"},
                workspace_id="42",
            ),
        )

        hooks.on_command_completed("inv-1", True, 1.0)
        print("MAIN_EXIT", flush=True)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
        timeout=5,
    )

    assert result.stdout.splitlines() == ["POST_RUN_START", "POST_RUN_DONE", "MAIN_EXIT"]
