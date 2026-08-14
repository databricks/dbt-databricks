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


def _post_run_log():
    return models.TelemetryLog(
        invocation_id="inv-2",
        adapter_version="1.2.3",
        dbt_core_version="1.12.0",
        event_type=models.EVENT_TYPE_POST_RUN,
        post_run=models.PostRunPayload(
            run_outcome=models.RunOutcome(
                invocation_status=models.INVOCATION_STATUS_HANDLED_ERROR,
                termination_reason=models.TERMINATION_REASON_NORMAL,
                invocation_duration_ms=1234,
                result_aggregates_available=True,
            ),
            selected_resources=10,
            result_counts=models.NodeStatusCounts(total=8, success=6, fail=1, pass_=5),
            results_by_resource_type=[
                models.ResourceOutcomeStats(
                    resource_type=models.RESOURCE_TYPE_MODEL,
                    status_counts=models.NodeStatusCounts(total=6, success=6),
                )
            ],
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

    def test_post_parse_omits_unset_phase(self):
        entry = json.loads(encoder.encode_request(_log(), "e")["protoLogs"][0])["entry"][
            "dbt_databricks_telemetry_log"
        ]
        assert "post_parse" in entry
        assert "post_run" not in entry


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
