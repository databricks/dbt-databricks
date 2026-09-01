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
