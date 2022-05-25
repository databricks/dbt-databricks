import unittest

from jinja2.runtime import Undefined

from dbt.adapters.databricks.relation import DatabricksRelation


class TestDatabricksRelation(unittest.TestCase):
    def test_pre_deserialize(self):
        data = {
            "quote_policy": {"database": False, "schema": False, "identifier": False},
            "path": {
                "database": "some_schema",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        self.assertEqual(relation.database, "some_schema")
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
