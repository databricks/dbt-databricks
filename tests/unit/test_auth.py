import unittest
from dbt.adapters.databricks.connections import DatabricksCredentials
import keyring.backend
import pytest


@pytest.mark.skip(reason="Need to mock requests to OIDC")
class TestM2MAuth(unittest.TestCase):
    def test_m2m(self):
        host = "my.cloud.databricks.com"
        creds = DatabricksCredentials(
            host=host,
            http_path="http://foo",
            client_id="my-client-id",
            client_secret="my-client-secret",
            database="andre",
            schema="dbt",
        )
        provider = creds.authenticate(None)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)


@pytest.mark.skip(reason="Need to mock requests to OIDC and mock opening browser")
class TestU2MAuth(unittest.TestCase):
    def test_u2m(self):
        host = "my.cloud.databricks.com"
        creds = DatabricksCredentials(
            host=host, database="andre", http_path="http://foo", schema="dbt"
        )
        provider = creds.authenticate(None)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)


class TestTokenAuth(unittest.TestCase):
    def test_token(self):
        host = "my.cloud.databricks.com"
        creds = DatabricksCredentials(
            host=host, token="foo", database="andre", http_path="http://foo", schema="dbt"
        )
        provider = creds.authenticate(None)
        self.assertIsNotNone(provider)
        headers_fn = provider()
        headers = headers_fn()
        self.assertIsNotNone(headers)

        raw = provider.as_dict()
        self.assertIsNotNone(raw)

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)


class TestShardedPassword(unittest.TestCase):
    def test_store_and_delete_short_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        service = "dbt-databricks"
        host = "my.cloud.databricks.com"
        long_password = "x" * 10

        creds = DatabricksCredentials(
            host=host, token="foo", database="andre", http_path="http://foo", schema="dbt"
        )
        creds.set_sharded_password(service, host, long_password)

        retrieved_password = creds.get_sharded_password(service, host)
        self.assertEqual(long_password, retrieved_password)

        # delete password
        creds.delete_sharded_password(service, host)
        retrieved_password = creds.get_sharded_password(service, host)
        self.assertIsNone(retrieved_password)

    def test_store_and_delete_long_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        service = "dbt-databricks"
        host = "my.cloud.databricks.com"
        long_password = "x" * 3000

        creds = DatabricksCredentials(
            host=host, token="foo", database="andre", http_path="http://foo", schema="dbt"
        )
        creds.set_sharded_password(service, host, long_password)

        retrieved_password = creds.get_sharded_password(service, host)
        self.assertEqual(long_password, retrieved_password)

        # delete password
        creds.delete_sharded_password(service, host)
        retrieved_password = creds.get_sharded_password(service, host)
        self.assertIsNone(retrieved_password)


class MockKeyring(keyring.backend.KeyringBackend):
    def __init__(self):
        self.file_location = self._generate_test_root_dir()

    def priority(self):
        return 1

    def _generate_test_root_dir(self):
        import tempfile
        return tempfile.mkdtemp(prefix="dbt-unit-test-")

    def file_path(self, servicename, username):
        from os.path import join

        file_location = self.file_location
        file_name = f"{servicename}_{username}.txt"
        return join(file_location, file_name)

    def set_password(self, servicename, username, password):
        file_path = self.file_path(servicename, username)

        with open(file_path, "w") as file:
            file.write(password)

    def get_password(self, servicename, username):
        import os

        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r") as file:
            password = file.read()

        return password

    def delete_password(self, servicename, username):
        import os

        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        os.remove(file_path)
