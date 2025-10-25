from dbt.adapters.databricks.catalogs._relation import DatabricksCatalogRelation


class TestDatabricksCatalogRelation:
    """Test the DatabricksCatalogRelation class, particularly the location property."""

    def test_location__both_set(self):
        """Test that location is properly computed when both location_root and location_path are set."""
        relation = DatabricksCatalogRelation(
            external_volume="s3://my-bucket/path",
            location_path="my_table",
        )
        assert relation.location == "s3://my-bucket/path/my_table"

    def test_location__only_root_set(self):
        """Test that location returns None when only location_root is set."""
        relation = DatabricksCatalogRelation(
            external_volume="s3://my-bucket/path",
            location_path=None,
        )
        assert relation.location is None

    def test_location__only_path_set(self):
        """Test that location returns None when only location_path is set."""
        relation = DatabricksCatalogRelation(
            external_volume=None,
            location_path="my_table",
        )
        assert relation.location is None

    def test_location__both_none(self):
        """Test that location returns None when both are None."""
        relation = DatabricksCatalogRelation(
            external_volume=None,
            location_path=None,
        )
        assert relation.location is None

    def test_location__empty_string_root(self):
        """Test that location returns None when location_root is an empty string."""
        relation = DatabricksCatalogRelation(
            external_volume="",
            location_path="my_table",
        )
        assert relation.location is None

    def test_location__empty_string_path(self):
        """Test that location returns None when location_path is an empty string."""
        relation = DatabricksCatalogRelation(
            external_volume="s3://my-bucket/path",
            location_path="",
        )
        assert relation.location is None

    def test_location__both_empty_strings(self):
        """Test that location returns None when both are empty strings."""
        relation = DatabricksCatalogRelation(
            external_volume="",
            location_path="",
        )
        assert relation.location is None

    def test_location__whitespace_root(self):
        """Test that location handles whitespace in location_root."""
        relation = DatabricksCatalogRelation(
            external_volume=" ",
            location_path="my_table",
        )
        # posixpath.join will handle this, so it should return " /my_table" or similar
        # but we expect the location to be truthy (not None)
        assert relation.location is not None

    def test_location__root_with_full_path(self):
        """Test location with a full database/schema/identifier path."""
        relation = DatabricksCatalogRelation(
            external_volume="s3://my-bucket",
            location_path="my_catalog/my_schema/my_table",
        )
        assert relation.location == "s3://my-bucket/my_catalog/my_schema/my_table"

    def test_location_root_property(self):
        """Test that location_root property correctly accesses external_volume."""
        relation = DatabricksCatalogRelation(external_volume="s3://my-bucket")
        assert relation.location_root == "s3://my-bucket"

    def test_location_root_setter(self):
        """Test that location_root setter correctly sets external_volume."""
        relation = DatabricksCatalogRelation()
        relation.location_root = "s3://my-bucket"
        assert relation.external_volume == "s3://my-bucket"
        assert relation.location_root == "s3://my-bucket"
