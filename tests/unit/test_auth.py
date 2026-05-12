import os
import tempfile
from os.path import join
from unittest.mock import MagicMock, patch

import keyring.backend
import pytest
from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.credentials import (
    CLIENT_ID,
    DatabricksCredentials,
)

_HOST = "my.cloud.databricks.com"
_HTTP_PATH = "/sql/1.0/warehouses/abc"


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
        credentialManager = creds.authenticate()
        provider = credentialManager.credentials_provider()
        assert provider is not None

        headers_fn = provider
        headers = headers_fn()
        assert headers is not None

        raw = credentialManager._config.as_dict()
        assert raw is not None

        assert headers == {"Authorization": "Bearer foo"}


@pytest.mark.skip(reason="Cache moved to databricks sdk TokenCache")
class TestShardedPassword:
    def test_store_and_delete_short_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        service = "dbt-databricks"
        host = "my.cloud.databricks.com"
        long_password = "x" * 10

        creds = DatabricksCredentials(
            host=host,
            token="foo",
            database="andre",
            http_path="http://foo",
            schema="dbt",
        )
        creds.set_sharded_password(service, host, long_password)

        retrieved_password = creds.get_sharded_password(service, host)
        assert long_password == retrieved_password

        # delete password
        creds.delete_sharded_password(service, host)
        retrieved_password = creds.get_sharded_password(service, host)
        assert retrieved_password is None

    def test_store_and_delete_long_password(self):
        # set the keyring to mock class
        keyring.set_keyring(MockKeyring())

        service = "dbt-databricks"
        host = "my.cloud.databricks.com"
        long_password = "x" * 3000

        creds = DatabricksCredentials(
            host=host,
            token="foo",
            database="andre",
            http_path="http://foo",
            schema="dbt",
        )
        creds.set_sharded_password(service, host, long_password)

        retrieved_password = creds.get_sharded_password(service, host)
        assert long_password == retrieved_password

        # delete password
        creds.delete_sharded_password(service, host)
        retrieved_password = creds.get_sharded_password(service, host)
        assert retrieved_password is None


@pytest.mark.skip(reason="Cache moved to databricks sdk TokenCache")
class MockKeyring(keyring.backend.KeyringBackend):
    def __init__(self):
        self.file_location = self._generate_test_root_dir()

    def priority(self):
        return 1

    def _generate_test_root_dir(self):
        return tempfile.mkdtemp(prefix="dbt-unit-test-")

    def file_path(self, servicename, username):
        file_location = self.file_location
        file_name = f"{servicename}_{username}.txt"
        return join(file_location, file_name)

    def set_password(self, servicename, username, password):
        file_path = self.file_path(servicename, username)

        with open(file_path, "w") as file:
            file.write(password)

    def get_password(self, servicename, username):
        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        with open(file_path) as file:
            password = file.read()

        return password

    def delete_password(self, servicename, username):
        file_path = self.file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        os.remove(file_path)


class TestAuthDispatch:
    """Parametrized tests asserting the exact kwargs passed to databricks.sdk.core.Config.

    Covers every dispatch path supported by the legacy code. Cases whose expected
    kwargs change after the refactor are explicitly marked so the delta is visible
    in the commit that updates them.
    """

    @pytest.mark.parametrize(
        "creds_kwargs,expected_kwargs",
        [
            # ---- PAT — unchanged ----
            pytest.param(
                dict(token="mytoken"),
                dict(host=_HOST, token="mytoken"),
                id="pat",
            ),
            # ---- Azure Service Principal (dedicated fields) — unchanged ----
            pytest.param(
                dict(azure_client_id="az-id", azure_client_secret="az-secret"),
                dict(host=_HOST, auth_type="azure-client-secret", azure_client_id="az-id", azure_client_secret="az-secret"),
                id="azure_sp",
            ),
            # ---- Legacy heuristic: client_secret without auth_type or azure fields — unchanged ----
            # "dose" prefix identifies a Databricks OAuth secret → oauth-m2m first.
            pytest.param(
                dict(client_id="my-sp", client_secret="dose_secret"),
                dict(host=_HOST, auth_type="oauth-m2m", client_id="my-sp", client_secret="dose_secret"),
                id="legacy_heuristic_dose_prefix",
            ),
            # Non-dose prefix (e.g. Azure SP client secret) → legacy-azure-client-secret first.
            pytest.param(
                dict(client_id="my-sp", client_secret="azure_secret"),
                dict(host=_HOST, auth_type="azure-client-secret", azure_client_id="my-sp", azure_client_secret="azure_secret"),
                id="legacy_heuristic_nondose_prefix",
            ),
            # ---- OAuth external-browser — Config kwargs change in the refactor ----
            # Legacy: no-client-secret path always calls authenticate_with_external_browser
            # which passes client_secret="" explicitly and ignores auth_type in dispatch.
            pytest.param(
                dict(auth_type="oauth"),
                dict(host=_HOST, auth_type="external-browser", client_id=CLIENT_ID, client_secret=""),
                id="oauth_alias",
            ),
            pytest.param(
                dict(auth_type="oauth", client_id="my-app"),
                dict(host=_HOST, auth_type="external-browser", client_id="my-app", client_secret=""),
                id="oauth_alias_with_custom_client_id",
            ),
            # Legacy: no credentials → same external-browser fallback with empty client_secret.
            pytest.param(
                dict(),
                dict(host=_HOST, auth_type="external-browser", client_id=CLIENT_ID, client_secret=""),
                id="no_credentials",
            ),
        ],
    )
    def test_config_kwargs(self, creds_kwargs, expected_kwargs):
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host=_HOST,
                http_path=_HTTP_PATH,
                database="db",
                schema="sch",
                **creds_kwargs,
            )
        assert mock_config.call_args.kwargs == expected_kwargs


class TestValidateCreds:
    BASE = dict(host=_HOST, http_path=_HTTP_PATH)

    def _creds(self, **kwargs):
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            return DatabricksCredentials(database="db", schema="sch", **self.BASE, **kwargs)

    def test_token_valid(self):
        self._creds(token="mytoken").validate_creds()

    def test_oauth_auth_type_valid(self):
        """auth_type='oauth' is the documented U2M alias."""
        self._creds(auth_type="oauth").validate_creds()

    def test_host_required(self):
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            with pytest.raises(DbtConfigError, match="host"):
                DatabricksCredentials(
                    database="db", schema="sch", http_path=_HTTP_PATH, token="t"
                ).validate_creds()

    def test_http_path_required(self):
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            with pytest.raises(DbtConfigError, match="http_path"):
                DatabricksCredentials(
                    database="db", schema="sch", host=_HOST, token="t"
                ).validate_creds()

    def test_client_id_required_when_client_secret_present(self):
        with pytest.raises(DbtConfigError, match="client_id"):
            self._creds(auth_type="oauth", client_secret="secret").validate_creds()

    def test_azure_credentials_must_be_paired(self):
        with pytest.raises(DbtConfigError, match="azure_client"):
            self._creds(token="t", azure_client_id="id").validate_creds()
