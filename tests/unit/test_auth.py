import unittest
import os
from dbt.adapters.databricks.auth import authenticate, from_dict
from dbt.adapters.databricks.connections import DatabricksCredentials


class TestM2MAuth(unittest.TestCase):
    def test_m2m(self):
        host = os.getenv("DBT_DATABRICKS_HOST_NAME")
        client_id = os.getenv("DBT_DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DBT_DATABRICKS_CLIENT_SECRET")
        creds = DatabricksCredentials(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            database="andre",
            schema="dbt",
        )
        provider = authenticate(creds)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = from_dict(creds, raw)
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)


class TestU2MAuth(unittest.TestCase):
    def test_u2m(self):
        host = "e2-dogfood.staging.cloud.databricks.com"
        creds = DatabricksCredentials(host=host, database="andre", schema="dbt")
        provider = authenticate(creds)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = from_dict(creds, raw)
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)


class TestTokenAuth(unittest.TestCase):
    def test_u2m(self):
        host = "e2-dogfood.staging.cloud.databricks.com"
        creds = DatabricksCredentials(host=host, token="foo", database="andre", schema="dbt")
        provider = authenticate(creds)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = from_dict(creds, raw)
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)
