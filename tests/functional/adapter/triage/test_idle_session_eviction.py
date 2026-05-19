"""Triage functional test: verifies transparent recovery from server-side
session eviction on a live all-purpose compute cluster.

Pre-fix behaviour (the failure-mode signature this test was authored against):
when a Databricks Thrift session is evicted server-side (idle timeout, cluster
restart, etc.), the next RPC against the session returns
`DatabaseError: Invalid SessionHandle: SessionHandle [<uuid>]`. dbt surfaces
that as a `DbtDatabaseError` model failure — no recovery.

Post-fix behaviour (what this test asserts): the adapter's
`DatabricksHandle._safe_execute` recognises the eviction error via message
inspection (see `_is_stale_session_error`), calls `_reopen()` to get a fresh
underlying connection, and retries the failed query exactly once. The recovery
is logged at INFO level. The caller sees no error.

Why force-evict instead of natural idle: the cluster's idle-eviction window
varies (observed 16-30 min depending on cluster state, DBR version, workload).
A natural-idle test is wall-clock-expensive and non-deterministic. We instead
trigger eviction synchronously by sending `CloseSession` directly to the
backend via the original session's handle — the server destroys the session,
the client-side `is_open` flag stays True (mimicking eviction-while-idle), and
the next execute hits the same error path as natural eviction. Sub-second.

Run::

    DBT_DATABRICKS_PROFILE=databricks_cluster \
    hatch run pytest \
      tests/functional/adapter/triage/test_idle_session_eviction.py -v -s
"""

import pytest


pytestmark = [
    pytest.mark.skip_profile("databricks_uc_sql_endpoint"),
]


class TestSessionEvictionRecovery:
    def test_force_evicted_session_recovers_via_auto_reopen(self, project):
        """Force-evict the session server-side, then run a query through the
        adapter and assert: it succeeds, AND the new session_id differs from
        the original.

        These two facts together prove the recovery executed: the only way the
        post-eviction query can succeed AND show a different session_id is for
        `_safe_execute` to have caught the eviction error and called
        `_reopen()`. We deliberately don't assert on the "Reopened after
        eviction" log line because dbt's `AdapterLogger` event system writes
        through internal channels that pytest's capsys/capfd/caplog don't
        intercept reliably — making the log a flaky signal even when the
        behaviour is correct.

        Pre-fix expectation (negative control for the regression): without the
        adapter's recovery path, the second assertion (and likely the first)
        would fail.
        """
        with project.adapter.connection_named("triage-force-evict"):
            conn = project.adapter.connections.get_thread_connection()
            handle = conn.handle  # forces LazyHandle.open() -> opens session

            # Warmup query so the session is fully materialised server-side.
            cursor = handle.execute("SELECT 1")
            assert cursor.fetchone() is not None
            original_session_id = handle.session_id
            assert original_session_id, "session must be open before forced eviction"

            # Force-evict: send CloseSession to the backend directly, bypassing
            # Session.close() so the client-side `is_open` flag stays True.
            # This mimics the on-wire state of an idle-evicted session: the
            # client believes the session is alive; the server has dropped it.
            print(f"\n[triage] forcing eviction of session {original_session_id}")
            sql_session = handle._conn.session
            sql_session.backend.close_session(sql_session._session_id)

            # Probe: with the fix, _safe_execute catches the resulting
            # DatabaseError, calls _reopen(), retries on a fresh session.
            print(f"[triage] probing post-eviction with SELECT 2")
            probe_cursor = handle.execute("SELECT 2")
            probe_row = probe_cursor.fetchone()
            new_session_id = handle.session_id
            print(
                f"[triage] probe row={probe_row}, "
                f"new session={new_session_id}, "
                f"changed? {new_session_id != original_session_id}"
            )

        # Probe must have succeeded
        assert probe_row is not None
        # And it must have run on a NEW session (proves the reopen happened)
        assert new_session_id is not None and new_session_id != original_session_id, (
            f"Expected session_id to change after auto-recovery; got "
            f"pre={original_session_id} post={new_session_id}"
        )

