import json

from dbt.adapters.databricks.telemetry import encoder, models


def _log():
    return models.TelemetryLog(
        invocation_id="inv-1",
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        post_parse=models.PostParsePayload(
            invocation_config=models.InvocationConfig(thread_count=4, dbt_command="RUN"),
            manifest_stats=models.ManifestStats(enabled_total=models.ResourceCounts(model_count=3)),
            connection_config=models.ConnectionConfig(
                default_compute_type=models.COMPUTE_TYPE_SQL_WAREHOUSE,
                configured_auth_family=models.AUTH_FAMILY_PAT,
            ),
            project_config=models.ProjectConfig(use_materialization_v2=True),
        ),
    )


class TestEncoder:
    def test_request_envelope(self):
        body = encoder.encode_request(_log(), "evt-1", workspace_id="42")
        assert body["items"] == []
        assert len(body["protoLogs"]) == 1
        assert isinstance(body["uploadTime"], int)

    def test_uses_dedicated_entry_field(self):
        fe = json.loads(encoder.encode_request(_log(), "evt-1")["protoLogs"][0])
        assert "dbt_databricks_telemetry_log" in fe["entry"]

    def test_event_shape_and_enum_names(self):
        fe = json.loads(encoder.encode_request(_log(), "evt-1")["protoLogs"][0])
        entry = fe["entry"]["dbt_databricks_telemetry_log"]
        assert entry["event_type"] == "POST_PARSE"
        assert entry["invocation_id"] == "inv-1"
        cc = entry["post_parse"]["connection_config"]
        assert cc["default_compute_type"] == "SQL_WAREHOUSE"
        assert cc["configured_auth_family"] == "PAT"
        assert entry["post_parse"]["manifest_stats"]["enabled_total"]["model_count"] == 3
        assert entry["post_parse"]["project_config"]["use_materialization_v2"] is True

    def test_stable_event_id(self):
        fe = json.loads(encoder.encode_request(_log(), "stable-id")["protoLogs"][0])
        assert fe["frontend_log_event_id"] == "stable-id"

    def test_numeric_workspace_id_is_coerced(self):
        fe = json.loads(encoder.encode_request(_log(), "e", workspace_id="42")["protoLogs"][0])
        assert fe["workspace_id"] == 42

    def test_non_numeric_workspace_id_is_omitted(self):
        fe = json.loads(encoder.encode_request(_log(), "e", workspace_id="abc")["protoLogs"][0])
        assert "workspace_id" not in fe

    def test_missing_workspace_id_is_omitted(self):
        fe = json.loads(encoder.encode_request(_log(), "e")["protoLogs"][0])
        assert "workspace_id" not in fe
