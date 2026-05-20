from dbt.adapters.databricks.spog.extract import extract_workspace_id


class TestExtractWorkspaceId:
    def test_warehouse_path_with_o_param(self):
        assert (
            extract_workspace_id("/sql/1.0/warehouses/abc123?o=6436897454825492")
            == "6436897454825492"
        )

    def test_cluster_path_with_o_param(self):
        assert (
            extract_workspace_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=6436897454825492"
            )
            == "6436897454825492"
        )

    def test_no_query_string(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123") is None

    def test_query_string_no_o_param(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123?other=value") is None

    def test_multiple_query_params(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123?o=12345&ts=1") == "12345"

    def test_o_param_not_first(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123?ts=1&o=12345") == "12345"

    def test_empty_string(self):
        assert extract_workspace_id("") is None

    def test_none_input(self):
        assert extract_workspace_id(None) is None

    def test_duplicate_o_params_returns_first(self):
        # parse_qs returns ['a', 'b']; we take the first to be deterministic
        assert extract_workspace_id("/path?o=a&o=b") == "a"

    def test_cluster_path_without_o_param_extracts_from_path(self):
        # Cluster paths embed the workspace id in /o/<id>/. SPOG hosts route
        # correctly with these because the workspace is already in the URL.
        assert (
            extract_workspace_id("/sql/protocolv1/o/6436897454825492/1214-195625-oc3mas1h")
            == "6436897454825492"
        )

    def test_cluster_path_no_leading_slash_extracts_from_path(self):
        assert (
            extract_workspace_id("sql/protocolv1/o/6436897454825492/1214-195625-oc3mas1h")
            == "6436897454825492"
        )

    def test_warehouse_path_without_o_returns_none(self):
        # Warehouse path with no ?o= encodes no workspace id anywhere.
        # SPOG decision matrix will treat this as misconfig on a SPOG host.
        assert extract_workspace_id("/sql/1.0/warehouses/abc123") is None
