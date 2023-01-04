import unittest

from jinja2.runtime import Undefined

from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksQuotePolicy


class TestDatabricksRelation(unittest.TestCase):
    def test_pre_deserialize(self):
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
        self.assertEqual(relation.database, "some_database")
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

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
        self.assertIsNone(relation.database)
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        self.assertIsNone(relation.database)
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "database": Undefined(),
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        self.assertIsNone(relation.database)
        self.assertEqual(relation.schema, "some_schema")
        self.assertEqual(relation.identifier, "some_table")

    def test_render(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        self.assertEqual(
            relation.get_default_quote_policy(), DatabricksQuotePolicy(True, True, True)
        )
        self.assertEqual(relation.render(), "`some_database`.`some_schema`.`some_table`")

        data = {
            "path": {
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        self.assertEqual(relation.render(), "`some_schema`.`some_table`")
