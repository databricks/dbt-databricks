import pytest

from dbt.adapters.databricks.credentials import DatabricksCredentials


@pytest.mark.skip(reason="Need to mock requests to OIDC")
class TestM2MAuth:
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
        assert provider is not None

        headers_fn = provider()
        headers = headers_fn()
        assert headers is not None

        raw = provider.as_dict()
        assert raw is not None

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        assert headers == headers2


@pytest.mark.skip(reason="Need to mock requests to OIDC and mock opening browser")
class TestU2MAuth:
    def test_u2m(self):
        host = "my.cloud.databricks.com"
        creds = DatabricksCredentials(
            host=host, database="andre", http_path="http://foo", schema="dbt"
        )
        provider = creds.authenticate(None)
        assert provider is not None

        headers_fn = provider()
        headers = headers_fn()
        assert headers is not None

        raw = provider.as_dict()
        assert raw is not None

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        assert headers == headers2


@pytest.mark.skip(reason="Broken after rewriting auth")
class TestTokenAuth:
    def test_token(self):
        host = "my.cloud.databricks.com"
        creds = DatabricksCredentials(
            host=host,
            token="foo",
            database="andre",
            http_path="http://foo",
            schema="dbt",
        )
        provider = creds.authenticate(None)
        assert provider is not None

        headers_fn = provider()
        headers = headers_fn()
        assert headers is not None

        raw = provider.as_dict()
        assert raw is not None

        provider_b = creds._provider_from_dict()
        headers_fn2 = provider_b()
        headers2 = headers_fn2()
        self.assertEqual(headers, headers2)
