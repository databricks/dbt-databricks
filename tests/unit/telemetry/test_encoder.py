import json

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


class TestEncoder:
    def test_uses_dedicated_entry_field(self):
        fe = json.loads(encoder.encode_request(_log(), "evt-1")["protoLogs"][0])
        assert "dbt_databricks_telemetry_log" in fe["entry"]

    def test_post_parse_contains_every_proto_field(self):
        entry = json.loads(encoder.encode_request(_log(), "evt-1")["protoLogs"][0])["entry"][
            "dbt_databricks_telemetry_log"
        ]
        assert set(entry) == {
            "invocation_id",
            "event_type",
            "adapter_version",
            "dbt_core_version",
            "post_parse",
        }
        assert set(entry["post_parse"]) == {
            "invocation_config",
            "manifest_stats",
            "connection_config",
            "project_config",
        }
        post_parse = entry["post_parse"]
        assert set(post_parse["invocation_config"]) == {
            "thread_count",
            "dbt_command",
            "full_refresh",
            "empty",
            "fail_fast",
            "warn_error_policy",
        }
        assert set(post_parse["connection_config"]) == {
            "default_compute_type",
            "configured_auth_family",
            "named_compute_count",
            "spog_routing_configured",
            "use_kernel",
        }
        assert set(post_parse["project_config"]) == {
            "use_user_folder_for_python",
            "use_materialization_v2",
            "use_replace_on_for_insert_overwrite",
            "use_managed_iceberg",
            "use_concurrent_microbatch",
            "use_describe_as_json_for_relation_metadata",
        }
        assert set(post_parse["manifest_stats"]) == {
            "enabled_total",
            "enabled_root_project",
            "enabled_installed_packages",
        }
        assert set(post_parse["manifest_stats"]["enabled_total"]) == {
            "model_count",
            "data_test_count",
            "generic_data_test_count",
            "seed_count",
            "snapshot_count",
            "source_count",
            "function_count",
            "exposure_count",
            "saved_query_count",
            "other_count",
            "unit_test_count",
        }

    def test_numeric_workspace_id_is_coerced(self):
        fe = json.loads(encoder.encode_request(_log(), "e", workspace_id="42")["protoLogs"][0])
        assert fe["workspace_id"] == 42

    def test_non_numeric_workspace_id_is_omitted(self):
        fe = json.loads(encoder.encode_request(_log(), "e", workspace_id="abc")["protoLogs"][0])
        assert "workspace_id" not in fe

    def test_post_parse_omits_unset_phase(self):
        entry = json.loads(encoder.encode_request(_log(), "e")["protoLogs"][0])["entry"][
            "dbt_databricks_telemetry_log"
        ]
        assert "post_parse" in entry
        assert "post_run" not in entry

    def test_timestamp_millis_uses_event_time_not_upload_time(self, monkeypatch):
        monkeypatch.setattr(encoder.time, "time", lambda: 40.0)
        body = encoder.encode_request(_log(), "e", event_timestamp_millis=10_000)
        fe = json.loads(body["protoLogs"][0])
        assert fe["context"]["client_context"]["timestamp_millis"] == 10_000
        assert body["uploadTime"] == 40_000


class TestPostRunEncoder:
    def _entry(self):
        fe = json.loads(encoder.encode_request(_post_run_log(), "e")["protoLogs"][0])
        return fe["entry"]["dbt_databricks_telemetry_log"]

    def test_only_post_run_phase(self):
        entry = self._entry()
        assert entry["event_type"] == "POST_RUN"
        assert "post_run" in entry
        assert "post_parse" not in entry

    def test_pass_field_uses_proto_name(self):
        rc = self._entry()["post_run"]["result_counts"]
        assert rc["pass"] == 5
        assert "pass_" not in rc

    def test_outcome_and_resource_enum_names(self):
        post_run = self._entry()["post_run"]
        assert post_run["run_outcome"]["invocation_status"] == "HANDLED_ERROR"
        assert post_run["run_outcome"]["invocation_duration_ms"] == 1234
        assert post_run["results_by_resource_type"][0]["resource_type"] == "MODEL"

    def test_post_run_contains_every_proto_field(self):
        post_run = self._entry()["post_run"]
        assert set(post_run) == {
            "run_outcome",
            "selected_resources",
            "expected_result_resources",
            "result_counts",
            "results_by_resource_type",
            "auxiliary_hook_results",
            "unknown_resource_type_results",
        }
        assert set(post_run["run_outcome"]) == {
            "invocation_status",
            "termination_reason",
            "invocation_duration_ms",
            "result_aggregates_available",
            "expected_result_coverage_complete",
        }
        assert set(post_run["result_counts"]) == {
            "total",
            "success",
            "error",
            "fail",
            "warn",
            "skipped",
            "partial_success",
            "pass",
            "runtime_error",
            "no_op",
            "reused",
        }
        assert set(post_run["results_by_resource_type"][0]) == {
            "resource_type",
            "status_counts",
        }

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
        entry = json.loads(encoder.encode_request(log, "e")["protoLogs"][0])["entry"][
            "dbt_databricks_telemetry_log"
        ]["post_run"]
        assert set(entry) == {"run_outcome", "expected_result_resources"}
        assert set(entry["run_outcome"]) == {
            "invocation_status",
            "termination_reason",
            "invocation_duration_ms",
            "result_aggregates_available",
        }
