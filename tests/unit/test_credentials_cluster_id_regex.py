from dbt.adapters.databricks.credentials import DatabricksCredentials


class TestExtractClusterId:
    def test_legacy_cluster_path(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_leading_slash(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_spog_query_param(self):
        # ?o=<workspace-id> must NOT be captured into the cluster id
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=6436897454825492"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_multiple_query_params(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=64&ts=1"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_warehouse_path_returns_none(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/1.0/warehouses/abc123?o=6436897454825492"
            )
            is None
        )
