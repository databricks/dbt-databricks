import pytest


class TestCheckSchemaExists:
    """Test the check_schema_exists adapter method."""

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create a test schema and clean it up after tests."""
        test_schema = f"{project.test_schema}_check_exists"

        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=test_schema,
            )
            # Drop if exists from previous run
            project.adapter.drop_schema(relation)
            # Create the test schema
            project.adapter.create_schema(relation)

        yield test_schema

        # Cleanup
        with project.adapter.connection_named("__test"):
            project.adapter.drop_schema(relation)

    def test_check_schema_exists(self, project, setUp):
        """Test that check_schema_exists correctly identifies existing and non-existing schemas."""
        test_schema = setUp

        with project.adapter.connection_named("__test"):
            # Test 1: Verify existing schema returns True
            exists = project.adapter.check_schema_exists(
                database=project.database, schema=test_schema
            )
            assert exists is True, (
                f"Expected schema '{test_schema}' to exist but check returned False"
            )

            # Test 2: Verify non-existing schema returns False
            non_existent_schema = "this_schema_definitely_does_not_exist_12345"
            exists = project.adapter.check_schema_exists(
                database=project.database, schema=non_existent_schema
            )
            assert exists is False, (
                f"Expected schema '{non_existent_schema}' to not exist but check returned True"
            )

            # Test 3: Verify existing default schema returns True (should always exist)
            exists = project.adapter.check_schema_exists(
                database=project.database, schema=project.test_schema
            )
            assert exists is True, (
                f"Expected default test schema '{project.test_schema}' to exist but check returned False"
            )
