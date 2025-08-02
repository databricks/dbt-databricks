import pytest
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.databricks import relation
from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    CustomConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.relation import DatabricksQuotePolicy, DatabricksRelation


class TestDatabricksRelation:
    def test_pre_deserialize__all_present(self):
        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.database == "some_database"
        assert relation.schema == "some_schema"
        assert relation.identifier == "some_table"

    def test_pre_deserialize__empty_database(self):
        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "database": None,
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.database is None
        assert relation.schema, "some_schema"
        assert relation.identifier, "some_table"

    def test_pre_deserialize__missing_database(self):
        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.database is None
        assert relation.schema, "some_schema"
        assert relation.identifier, "some_table"

    def test_render__all_present(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.get_default_quote_policy() == DatabricksQuotePolicy(True, True, True)
        assert relation.render() == "`some_database`.`some_schema`.`some_table`"

    def test_render__database_missing(self):
        data = {
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.render() == "`some_schema`.`some_table`"

    def test_matches__exact_match(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.matches("some_database", "some_schema", "some_table")

    def test_matches__capitalization_mismatch(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "SOME_TABLE",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.matches("some_database", "some_schema", "some_table")

    def test_matches__other_capitalization_mismatch(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.matches("some_database", "some_schema", "SOME_TABLE")

    def test_matches__capitalization_mismatch_all(self):
        data = {
            "path": {
                "database": "SOME_DATABASE",
                "schema": "SOME_SCHEMA",
                "identifier": "SOME_TABLE",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.matches("some_database", "some_schema", "some_table")

    def test_matches__capitalization_mismatch_all_other(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert relation.matches("SOME_DATABASE", "SOME_SCHEMA", "SOME_TABLE")

    def test_matches__mismatched_table(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert not relation.matches("SOME_DATABASE", "SOME_SCHEMA", "TABLE")

    def test_matches__other_mismatched_table(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        assert not relation.matches("some_database", "some_schema", "table")


class TestRelationsFunctions:
    @pytest.mark.parametrize(
        "database, expected",
        [(None, True), ("hive_metastore", True), ("not_hive", False)],
    )
    def test_is_hive_metastore(self, database, expected):
        assert relation.is_hive_metastore(database) is expected

    def test_is_external_table(self):
        relation = DatabricksRelation.create(
            identifier="external_table", databricks_table_type="external"
        )
        assert relation.is_external_table is True

    @pytest.mark.parametrize(
        "input, expected",
        [
            ([], set()),
            ([DatabricksRelation.create(identifier=None)], set()),
            (
                [
                    DatabricksRelation.create(identifier=None),
                    DatabricksRelation.create(identifier="test"),
                ],
                {"test"},
            ),
            (
                [
                    DatabricksRelation.create(identifier="test"),
                    DatabricksRelation.create(identifier="test"),
                ],
                {"test"},
            ),
        ],
    )
    def test_extract_identifiers(self, input, expected):
        assert relation.extract_identifiers(input) == expected


class TestConstraints:
    @pytest.fixture
    def relation(self):
        return DatabricksRelation.create()

    @pytest.fixture
    def custom_constraint(self):
        return CustomConstraint(type=ConstraintType.custom, expression="a > 1")

    @pytest.fixture
    def check_constraint(self):
        return CheckConstraint(type=ConstraintType.check, expression="a > 1")

    @pytest.fixture
    def pk_constraint(self):
        return PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["a"])

    def test_add_constraint__check_is_an_alter_constraint(self, relation, check_constraint):
        relation.add_constraint(check_constraint)
        assert relation.alter_constraints == [check_constraint]
        assert relation.create_constraints == []

    def test_add_constraint__other_constraints_are_create_constraints(
        self, relation, check_constraint, custom_constraint, pk_constraint
    ):
        relation.add_constraint(check_constraint)
        relation.add_constraint(custom_constraint)
        relation.add_constraint(pk_constraint)
        assert relation.alter_constraints == [check_constraint]
        assert relation.create_constraints == [custom_constraint, pk_constraint]

    def test_enrich_relation__returns_a_copy(self, relation):
        enriched = relation.enrich([])
        assert id(enriched) != id(relation)

    def test_enrich_relation__adds_constraints(self, relation, check_constraint, custom_constraint):
        enriched = relation.enrich([check_constraint, custom_constraint])
        assert enriched.alter_constraints == [check_constraint]
        assert enriched.create_constraints == [custom_constraint]

    def test_render_constraints_for_create__no_constraints(self, relation):
        assert relation.render_constraints_for_create() == ""

    def test_render_constraints_for_create__check_is_ignored(self, relation, check_constraint):
        relation.add_constraint(check_constraint)
        assert relation.render_constraints_for_create() == ""

    def test_render_constraints_for_create__with_constraints(
        self, relation, custom_constraint, pk_constraint
    ):
        relation.add_constraint(custom_constraint)
        relation.add_constraint(pk_constraint)
        assert relation.render_constraints_for_create() == "a > 1, PRIMARY KEY (a)"
