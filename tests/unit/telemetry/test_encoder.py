import json

import pytest

from dbt.adapters.databricks.telemetry import encoder, models


def _log():
    return models.TelemetryLog(
        invocation_id="inv-1",
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        post_parse=models.PostParsePayload(
            invocation_config=models.InvocationConfig(
                thread_count=4, dbt_command=models.DbtCommand.RUN
            ),
            manifest_stats=models.ManifestStats(enabled_total=models.ResourceCounts(model_count=3)),
            connection_config=models.ConnectionConfig(
                default_compute_type=models.ComputeType.SQL_WAREHOUSE,
                configured_auth_family=models.AuthFamily.PAT,
            ),
            project_config=models.ProjectConfig(use_materialization_v2=True),
        ),
    )


def _post_run_log():
    return models.TelemetryLog(
        invocation_id="inv-2",
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        event_type=models.EventType.POST_RUN,
        post_run=models.PostRunPayload(
            run_outcome=models.RunOutcome(
                invocation_status=models.InvocationStatus.HANDLED_ERROR,
                termination_reason=models.TerminationReason.NORMAL,
                invocation_duration_ms=1234,
                result_aggregates_available=True,
                expected_result_coverage_complete=True,
            ),
            selected_resources=10,
            result_counts=models.NodeStatusCounts(total=8, success=6, fail=1, pass_=5),
            results_by_resource_type=[
                models.ResourceOutcomeStats(
                    resource_type=models.ResourceType.MODEL,
                    status_counts=models.NodeStatusCounts(total=6, success=6),
                )
            ],
            auxiliary_hook_results=models.NodeStatusCounts(),
            unknown_resource_type_results=1,
        ),
    )


def _entry(log, workspace_id=None):
    fe = json.loads(encoder.encode_request(log, "evt-1", workspace_id=workspace_id)["protoLogs"][0])
    return fe, fe["entry"]["dbt_databricks_telemetry_log"]


class TestEncoder:
    def test_post_parse_envelope(self):
        fe, entry = _entry(_log())
        assert "dbt_databricks_telemetry_log" in fe["entry"]
        assert entry["event_type"] == "POST_PARSE"
        assert "post_run" not in entry
        assert entry["post_parse"]["invocation_config"]["dbt_command"] == "RUN"
        assert entry["post_parse"]["connection_config"]["default_compute_type"] == "SQL_WAREHOUSE"

    @pytest.mark.parametrize(
        "workspace_id, expected",
        [
            pytest.param("42", 42, id="numeric"),
            pytest.param("abc", None, id="non_numeric"),
        ],
    )
    def test_workspace_id_coercion(self, workspace_id, expected):
        fe, _ = _entry(_log(), workspace_id=workspace_id)
        assert fe.get("workspace_id") == expected


class TestPostRunEncoder:
    def test_post_run_wire_contract(self):
        _, entry = _entry(_post_run_log())
        assert entry["event_type"] == "POST_RUN"
        assert "post_parse" not in entry
        rc = entry["post_run"]["result_counts"]
        assert rc["pass"] == 5
        assert "pass_" not in rc
        assert entry["post_run"]["run_outcome"]["invocation_status"] == "HANDLED_ERROR"
        assert entry["post_run"]["results_by_resource_type"][0]["resource_type"] == "MODEL"

    def test_unavailable_aggregates_are_omitted(self):
        log = models.TelemetryLog(
            invocation_id="inv-3",
            adapter_version="1.2.3",
            dbt_core_version="1.12.0",
            event_type=models.EventType.POST_RUN,
            post_run=models.PostRunPayload(
                run_outcome=models.RunOutcome(result_aggregates_available=False),
                expected_result_resources=2,
            ),
        )
        _, entry = _entry(log)
        assert set(entry["post_run"]) == {"run_outcome", "expected_result_resources"}
        assert "expected_result_coverage_complete" not in entry["post_run"]["run_outcome"]
