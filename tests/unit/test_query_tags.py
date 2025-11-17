from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks.connections import QueryConfigUtils
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

    def test_parse_query_tags_rejects_non_string_values(self):
        """Test that non-string values raise an error."""
        tags_json_int = '{"team": "marketing", "cost_center": 3000}'
        expected_msg_int = (
            "query_tags values must be strings. Found int for key 'cost_center': 3000. "
            "Only string values are supported."
        )
        with pytest.raises(DbtValidationError, match=expected_msg_int):
            QueryTagsUtils.parse_query_tags(tags_json_int)

        tags_json_bool = '{"team": "marketing", "active": true}'
        expected_msg_bool = (
            "query_tags values must be strings. Found bool for key 'active': True. "
            "Only string values are supported."
        )
        with pytest.raises(DbtValidationError, match=expected_msg_bool):
            QueryTagsUtils.parse_query_tags(tags_json_bool)

        tags_json_null = '{"team": "marketing", "description": null}'
        expected_msg_null = (
            "query_tags values must be strings. Found NoneType for key 'description': None. "
            "Only string values are supported."
        )
        with pytest.raises(DbtValidationError, match=expected_msg_null):
            QueryTagsUtils.parse_query_tags(tags_json_null)

        tags_json_array = '{"team": "marketing", "tags": ["tag1", "tag2"]}'
        expected_msg_array = (
            "query_tags values must be strings. Found list for key 'tags': "
            r"\['tag1', 'tag2'\]. Only string values are supported."
        )
        with pytest.raises(DbtValidationError, match=expected_msg_array):
            QueryTagsUtils.parse_query_tags(tags_json_array)

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
        tags = {"@@dbt_model_name": "test", "team": "marketing"}
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

    def test_validate_query_tags_escapes_comma(self):
        """Test that commas in tag values are escaped."""
        tags = {"team": "marketing,sales"}
        QueryTagsUtils.validate_query_tags(tags)
        assert tags["team"] == "marketing\\,sales"

    def test_validate_query_tags_escapes_colon(self):
        """Test that colons in tag values are escaped."""
        tags = {"description": "project:alpha"}
        QueryTagsUtils.validate_query_tags(tags)
        assert tags["description"] == "project\\:alpha"

    def test_validate_query_tags_escapes_backslash(self):
        """Test that backslashes in tag values are escaped."""
        tags = {"path": "folder\\subfolder"}
        QueryTagsUtils.validate_query_tags(tags)
        assert tags["path"] == "folder\\\\subfolder"

    def test_validate_query_tags_escapes_multiple_special_chars(self):
        """Test that multiple special characters are all escaped."""
        tags = {"complex": "value:with,comma\\and\\backslash"}
        QueryTagsUtils.validate_query_tags(tags)
        assert tags["complex"] == "value\\:with\\,comma\\\\and\\\\backslash"

    def test_validate_query_tags_multiple_values_too_long(self):
        tags = {
            "long_tag_1": "a" * 129,
            "short_tag": "ok",
            "long_tag_2": "b" * 130,
        }
        expected_msg = (
            "Query tag values must be at most 128 characters. "
            "Following keys have values exceeding the limit: "
            "long_tag_1, long_tag_2."
        )
        with pytest.raises(DbtValidationError, match=expected_msg):
            QueryTagsUtils.validate_query_tags(tags)

    def test_validate_query_tags_value_after_escaping_too_long(self):
        """Test validation fails when tag value exceeds 128 chars after escaping."""
        # Create a value that's 127 characters but will exceed 128 after escaping
        # We need at least 64 commas (each comma becomes \\, adding 1 char)
        value_with_commas = ",".join(["a"] * 64)  # Creates "a,a,a..." with 63 commas
        # After escaping, each comma becomes \\, so length increases by 63
        tags = {"tag_with_commas": value_with_commas}

        # First it escapes, then it validates length
        expected_msg = (
            "Query tag values must be at most 128 characters. "
            "Following keys have values exceeding the limit: tag_with_commas."
        )
        with pytest.raises(DbtValidationError, match=expected_msg):
            QueryTagsUtils.validate_query_tags(tags)

    def test_merge_query_tags_precedence(self):
        """Test that model tags override connection tags."""
        connection_tags = {"team": "marketing", "cost_center": "3000"}
        model_tags = {"team": "content-marketing", "project": "analytics"}
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
        connection_tags = {"team": "marketing"}
        model_tags = {"project": "analytics"}
        default_tags = {"dbt_model_name": "test_model"}

        # This should work fine - default tags can use reserved keys
        result = QueryTagsUtils.merge_query_tags(connection_tags, model_tags, default_tags)
        assert "dbt_model_name" in result

        # But if user tries to use a reserved key, it should fail
        model_tags_with_reserved = {"@@dbt_model_name": "override_attempt"}
        with pytest.raises(DbtValidationError, match="Cannot use reserved query tag keys"):
            QueryTagsUtils.merge_query_tags(connection_tags, model_tags_with_reserved, default_tags)

    def test_merge_query_tags_too_many_total(self):
        """Test that too many total tags after merging raises error."""
        # Create tags that individually are under the limit but together exceed it
        connection_tags = {f"conn_tag_{i}": f"value_{i}" for i in range(10)}
        model_tags = {f"model_tag_{i}": f"value_{i}" for i in range(11)}
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
        assert result == ""


class TestQueryConfigUtils:
    """Test the QueryConfigUtils class."""

    def test_get_merged_query_tags(self):
        """Test getting merged query tags from context and credentials."""
        # Mock credentials
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = '{"team": "marketing", "cost_center": "3000"}'

        # Mock query header context
        query_header_context = Mock()
        query_header_context.model_name = "test_model"
        query_header_context.materialized = "table"
        query_header_context.model_query_tags_override = {
            "team": "content-marketing",
            "project": "analytics",
        }

        result = QueryConfigUtils.get_merged_query_tags(query_header_context, creds)

        # Check user-defined tags
        assert result["team"] == "content-marketing"  # Model overrides connection
        assert result["cost_center"] == "3000"  # From connection
        assert result["project"] == "analytics"  # From model

        # Check default tags
        assert "@@dbt_databricks_version" in result
        assert "@@dbt_core_version" in result
        assert result["@@dbt_materialized"] == "table"
        assert result["@@dbt_model_name"] == "test_model"

    def test_get_merged_query_tags_no_model_tags(self):
        """Test getting merged query tags with no model tags."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = '{"team": "marketing"}'

        # Mock query header context
        query_header_context = Mock()
        query_header_context.model_name = "test_model"
        query_header_context.materialized = "view"
        query_header_context.model_query_tags_override = None

        result = QueryConfigUtils.get_merged_query_tags(query_header_context, creds)

        # Check user-defined tags
        assert result["team"] == "marketing"

        # Check default tags
        assert "@@dbt_databricks_version" in result
        assert "@@dbt_core_version" in result
        assert result["@@dbt_materialized"] == "view"
        assert result["@@dbt_model_name"] == "test_model"

    def test_get_merged_query_tags_no_connection_tags(self):
        """Test getting merged query tags with no connection tags."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = None

        # Mock query header context
        query_header_context = Mock()
        query_header_context.model_name = "test_model"
        query_header_context.materialized = "incremental"
        query_header_context.model_query_tags_override = {"project": "analytics"}

        result = QueryConfigUtils.get_merged_query_tags(query_header_context, creds)

        # Check user-defined tags
        assert result["project"] == "analytics"

        # Check default tags
        assert "@@dbt_databricks_version" in result
        assert "@@dbt_core_version" in result
        assert result["@@dbt_materialized"] == "incremental"
        assert result["@@dbt_model_name"] == "test_model"

    def test_get_merged_query_tags_no_tags_at_all(self):
        """Test getting merged query tags with no tags from any source."""
        creds = Mock(spec=DatabricksCredentials)
        creds.query_tags = None

        # Mock query header context
        query_header_context = Mock()
        query_header_context.model_name = "test_model"
        query_header_context.materialized = "table"
        query_header_context.model_query_tags_override = None

        result = QueryConfigUtils.get_merged_query_tags(query_header_context, creds)

        # Check default tags only
        assert "@@dbt_databricks_version" in result
        assert "@@dbt_core_version" in result
        assert result["@@dbt_materialized"] == "table"
        assert result["@@dbt_model_name"] == "test_model"

        # Should only have default tags
        assert len(result) == 4
