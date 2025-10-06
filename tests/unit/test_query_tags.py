import json
from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks.connections import QueryConfigUtils, QueryContextWrapper
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.utils import QueryTagsUtils


class TestQueryTagsUtils:
    """Test the QueryTagsUtils class."""

    def test_parse_query_tags_empty(self):
        """Test parsing empty or None query tags."""
        assert QueryTagsUtils.parse_query_tags(None) == {}
        assert QueryTagsUtils.parse_query_tags("") == {}

    def test_parse_query_tags_valid_json(self):
        """Test parsing valid JSON query tags."""
        tags_json = '{"team": "marketing", "cost_center": "3000"}'
        expected = {"team": "marketing", "cost_center": "3000"}
        assert QueryTagsUtils.parse_query_tags(tags_json) == expected

    def test_parse_query_tags_converts_values_to_strings(self):
        """Test that all values are converted to strings."""
        tags_json = '{"team": "marketing", "cost_center": 3000, "active": true}'
        expected = {"team": "marketing", "cost_center": "3000", "active": "True"}
        assert QueryTagsUtils.parse_query_tags(tags_json) == expected

    def test_parse_query_tags_invalid_json(self):
        """Test parsing invalid JSON raises error."""
        with pytest.raises(DbtValidationError, match="Invalid JSON in query_tags"):
            QueryTagsUtils.parse_query_tags('{"invalid": json}')

    def test_parse_query_tags_non_dict(self):
        """Test parsing non-dictionary JSON raises error."""
        with pytest.raises(DbtValidationError, match="query_tags must be a JSON object"):
            QueryTagsUtils.parse_query_tags('["not", "a", "dict"]')

    def test_validate_query_tags_reserved_keys(self):
        """Test validation fails for reserved keys."""
        tags = {"dbt_model_name": "test", "team": "marketing"}
        with pytest.raises(DbtValidationError, match="Cannot use reserved query tag keys"):
            QueryTagsUtils.validate_query_tags(tags)

    def test_validate_query_tags_too_many_tags(self):
        """Test validation fails for too many tags."""
        tags = {f"tag_{i}": f"value_{i}" for i in range(25)}  # More than MAX_TAGS (20)
        with pytest.raises(DbtValidationError, match="Too many query tags"):
            QueryTagsUtils.validate_query_tags(tags)

    def test_validate_query_tags_valid(self):
        """Test validation passes for valid tags."""
        tags = {"team": "marketing", "cost_center": "3000"}
        # Should not raise any exception
        QueryTagsUtils.validate_query_tags(tags)

    def test_merge_query_tags_precedence(self):
        """Test that model tags override connection tags."""
        connection_tags = '{"team": "marketing", "cost_center": "3000"}'
        model_tags = '{"team": "content-marketing", "project": "analytics"}'
        default_tags = {"dbt_model_name": "test_model"}

        result = QueryTagsUtils.merge_query_tags(connection_tags, model_tags, default_tags)

        expected = {
            "dbt_model_name": "test_model",
            "team": "content-marketing",  # Model overrides connection
            "cost_center": "3000",  # From connection
            "project": "analytics",  # From model
        }
        assert result == expected

    def test_merge_query_tags_reserved_key_conflict(self):
        """Test that reserved keys in user tags raise error."""
        connection_tags = '{"team": "marketing"}'
        model_tags = '{"project": "analytics"}'
        default_tags = {"dbt_model_name": "test_model"}

        # This should work fine - default tags can use reserved keys
        result = QueryTagsUtils.merge_query_tags(connection_tags, model_tags, default_tags)
        assert "dbt_model_name" in result

        # But if user tries to use a reserved key, it should fail
        model_tags_with_reserved = '{"dbt_model_name": "override_attempt"}'
        with pytest.raises(DbtValidationError, match="Cannot use reserved query tag keys"):
            QueryTagsUtils.merge_query_tags(connection_tags, model_tags_with_reserved, default_tags)

    def test_merge_query_tags_too_many_total(self):
        """Test that too many total tags after merging raises error."""
        # Create tags that individually are under the limit but together exceed it
        connection_tags_dict = {f"conn_tag_{i}": f"value_{i}" for i in range(10)}
        model_tags_dict = {f"model_tag_{i}": f"value_{i}" for i in range(11)}

        connection_tags = json.dumps(connection_tags_dict)
        model_tags = json.dumps(model_tags_dict)
        default_tags = {"dbt_model_name": "test_model"}  # 1 default tag + 10 + 11 = 22 > 20

        with pytest.raises(DbtValidationError, match="Too many total query tags"):
            QueryTagsUtils.merge_query_tags(connection_tags, model_tags, default_tags)

    def test_format_query_tags_for_databricks(self):
        """Test formatting query tags for Databricks."""
        tags = {"team": "marketing", "cost_center": "3000"}
        result = QueryTagsUtils.format_query_tags_for_databricks(tags)

        # Should be formatted without quotes around keys/values
        expected = "team:marketing,cost_center:3000"
        assert result == expected

    def test_format_query_tags_for_databricks_empty(self):
        """Test formatting empty query tags."""
        result = QueryTagsUtils.format_query_tags_for_databricks({})
        assert result == "{}"


class TestQueryConfigUtils:
    """Test the QueryConfigUtils class."""

    def test_get_merged_query_tags(self):
        """Test getting merged query tags from context and credentials."""
        # Mock credentials
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = '{"team": "marketing", "cost_center": "3000"}'

        # Mock context
        context = QueryContextWrapper(
            relation_name="test_model",
            query_tags='{"team": "content-marketing", "project": "analytics"}',
            model_name="test_model",
            materialized="table",
            dbt_databricks_version="1.11.0a1",
            dbt_core_version="1.8.0",
        )

        result = QueryConfigUtils.get_merged_query_tags(context, creds)

        # Check user-defined tags
        assert result["team"] == "content-marketing"  # Model overrides connection
        assert result["cost_center"] == "3000"  # From connection
        assert result["project"] == "analytics"  # From model

        # Check default tags
        assert result["dbt_model_name"] == "test_model"
        assert result["dbt_databricks_version"] == "1.11.0a1"
        assert result["dbt_core_version"] == "1.8.0"
        assert result["dbt_materialized"] == "table"

    def test_get_merged_query_tags_no_model_tags(self):
        """Test getting merged query tags with no model tags."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = '{"team": "marketing"}'

        context = QueryContextWrapper(
            relation_name="test_model",
            query_tags=None,
            model_name="test_model",
            materialized="view",
            dbt_databricks_version="1.11.0a1",
            dbt_core_version="1.8.0",
        )

        result = QueryConfigUtils.get_merged_query_tags(context, creds)

        # Check user-defined tags
        assert result["team"] == "marketing"

        # Check default tags
        assert result["dbt_model_name"] == "test_model"
        assert result["dbt_databricks_version"] == "1.11.0a1"
        assert result["dbt_core_version"] == "1.8.0"
        assert result["dbt_materialized"] == "view"

    def test_get_merged_query_tags_no_connection_tags(self):
        """Test getting merged query tags with no connection tags."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = None

        context = QueryContextWrapper(
            relation_name="test_model",
            query_tags='{"project": "analytics"}',
            model_name="test_model",
            materialized="incremental",
            dbt_databricks_version="1.11.0a1",
            dbt_core_version="1.8.0",
        )

        result = QueryConfigUtils.get_merged_query_tags(context, creds)

        # Check user-defined tags
        assert result["project"] == "analytics"

        # Check default tags
        assert result["dbt_model_name"] == "test_model"
        assert result["dbt_databricks_version"] == "1.11.0a1"
        assert result["dbt_core_version"] == "1.8.0"
        assert result["dbt_materialized"] == "incremental"

    def test_get_merged_query_tags_no_tags_at_all(self):
        """Test getting merged query tags with no tags from any source."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = None

        context = QueryContextWrapper(
            relation_name="test_model",
            query_tags=None,
            model_name="test_model",
            materialized="table",
            dbt_databricks_version="1.11.0a1",
            dbt_core_version="1.8.0",
        )

        result = QueryConfigUtils.get_merged_query_tags(context, creds)

        # Check default tags only
        assert result["dbt_model_name"] == "test_model"
        assert result["dbt_databricks_version"] == "1.11.0a1"
        assert result["dbt_core_version"] == "1.8.0"
        assert result["dbt_materialized"] == "table"

        # Should only have default tags
        assert len(result) == 4
