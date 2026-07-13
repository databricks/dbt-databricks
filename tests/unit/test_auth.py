import os
import tempfile
from os.path import join
from unittest import mock

import keyring.backend
import pytest

from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)

_COMMON_KWARGS = {
    "host": "yourorg.databricks.com",
    "database": "andre",
    "http_path": "sql/protocolv1/o/1234567890123456/1234-cluster",
    "schema": "dbt",
}


class TestParseTimeIsOffline:
    """`dbt parse/list/compile` must stay fully offline. Building
    `DatabricksCredentials` is on that path, so for every supported auth
    method we verify that constructing the credentials never calls
    `Config()` (which is what triggers the SDK's network I/O)."""

    def test_pat_credentials_init_does_not_call_config(self):
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            DatabricksCredentials(token="foo", **_COMMON_KWARGS)
            mock_config.assert_not_called()

    def test_oauth_m2m_credentials_init_does_not_call_config(self):
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            DatabricksCredentials(
                client_id="cid",
                client_secret="dose-secret",
                **_COMMON_KWARGS,
            )
            mock_config.assert_not_called()

    def test_external_browser_credentials_init_does_not_call_config(self):
        # client_id with no client_secret triggers external-browser auth
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            DatabricksCredentials(client_id="cid", **_COMMON_KWARGS)
            mock_config.assert_not_called()

    def test_azure_client_secret_credentials_init_does_not_call_config(self):
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            DatabricksCredentials(
                azure_client_id="acid",
                azure_client_secret="asecret",
                **_COMMON_KWARGS,
            )
            mock_config.assert_not_called()


class TestEnsureConfigTriggersTheRightAuth:
    """Connect-time counterpart to TestParseTimeIsOffline: when something
    actually does need the config (e.g. opening a connection), `_ensure_config`
    must build it via the correct auth method per credential shape. This also
    exercises every branch of the selection logic in `_ensure_config`."""

    def test_pat_uses_pat_auth(self):
        creds = DatabricksCredentials(token="foo", **_COMMON_KWARGS)
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            creds.authenticate().config
            mock_config.assert_called_once_with(host=_COMMON_KWARGS["host"], token="foo")

    def test_explicit_azure_uses_azure_client_secret(self):
        creds = DatabricksCredentials(
            azure_client_id="acid",
            azure_client_secret="asecret",
            auth_type="oauth",
            **_COMMON_KWARGS,
        )
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            creds.authenticate().config
            mock_config.assert_called_once_with(
                host=_COMMON_KWARGS["host"],
                azure_client_id="acid",
                azure_client_secret="asecret",
                auth_type="azure-client-secret",
            )

    def test_client_id_only_uses_external_browser(self):
        creds = DatabricksCredentials(client_id="cid", auth_type="oauth", **_COMMON_KWARGS)
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            creds.authenticate().config
            mock_config.assert_called_once_with(
                host=_COMMON_KWARGS["host"],
                client_id="cid",
                client_secret="",
                auth_type="external-browser",
            )

    def test_dose_secret_tries_oauth_m2m_first(self):
        creds = DatabricksCredentials(
            client_id="cid",
            client_secret="dose-secret",
            auth_type="oauth",
            **_COMMON_KWARGS,
        )
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            creds.authenticate().config
            mock_config.assert_called_once_with(
                host=_COMMON_KWARGS["host"],
                client_id="cid",
                client_secret="dose-secret",
                auth_type="oauth-m2m",
            )

    def test_non_dose_secret_tries_legacy_azure_first(self):
        creds = DatabricksCredentials(
            client_id="cid",
            client_secret="plain-secret",
            auth_type="oauth",
            **_COMMON_KWARGS,
        )
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            creds.authenticate().config
            mock_config.assert_called_once_with(
                host=_COMMON_KWARGS["host"],
                azure_client_id="cid",
                azure_client_secret="plain-secret",
                auth_type="azure-client-secret",
            )

    def test_falls_back_to_second_method_when_first_raises(self):
        creds = DatabricksCredentials(
            client_id="cid",
            client_secret="plain-secret",
            auth_type="oauth",
            **_COMMON_KWARGS,
        )
        fake_config = mock.MagicMock()
        with mock.patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.side_effect = [RuntimeError("first method fails"), fake_config]
            creds.authenticate().config
            assert mock_config.call_count == 2
            # First call: legacy-azure (default order for non-dose secret).
            assert mock_config.call_args_list[0].kwargs["auth_type"] == "azure-client-secret"
            # Second call: oauth-m2m fallback.
            assert mock_config.call_args_list[1].kwargs["auth_type"] == "oauth-m2m"


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


class TestCredentialManagerWorkspaceId:
    """Verify workspace_id is extracted from http_path and plumbed into Config."""

    def _creds(self, http_path: str, token: str = "dapi-fake") -> DatabricksCredentials:
        return DatabricksCredentials(
            host="spog.example.com",
            http_path=http_path,
            token=token,
            database="main",
            schema="default",
        )

    def test_workspace_id_extracted_when_present(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=6436897454825492")
        mgr = DatabricksCredentialManager.create_from(creds)
        assert mgr.workspace_id == "6436897454825492"

    def test_workspace_id_none_when_absent(self):
        creds = self._creds("/sql/1.0/warehouses/abc")
        mgr = DatabricksCredentialManager.create_from(creds)
        assert mgr.workspace_id is None

    def test_pat_config_receives_workspace_id_when_supported(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=64")
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=True,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["token"] == "dapi-fake"
        assert kwargs["workspace_id"] == "64"

    def test_pat_config_no_workspace_id_when_unsupported(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=64")
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=False,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["token"] == "dapi-fake"
        assert "workspace_id" not in kwargs

    def test_pat_config_no_workspace_id_when_no_o_param(self):
        creds = self._creds("/sql/1.0/warehouses/abc")  # no ?o=
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=True,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["token"] == "dapi-fake"
        assert "workspace_id" not in kwargs

    def test_oauth_m2m_config_receives_workspace_id(self):
        creds = DatabricksCredentials(
            host="spog.example.com",
            http_path="/sql/1.0/warehouses/abc?o=64",
            client_id="cid",
            client_secret="csec",
            database="main",
            schema="default",
        )
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=True,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_oauth_m2m()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["client_id"] == "cid"
        assert kwargs["client_secret"] == "csec"
        assert kwargs["workspace_id"] == "64"
        assert kwargs["auth_type"] == "oauth-m2m"

    def test_azure_client_secret_config_receives_workspace_id(self):
        creds = DatabricksCredentials(
            host="spog.example.com",
            http_path="/sql/1.0/warehouses/abc?o=64",
            azure_client_id="az-cid",
            azure_client_secret="az-csec",
            database="main",
            schema="default",
        )
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=True,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_azure_client_secret()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["workspace_id"] == "64"
        assert kwargs["azure_client_id"] == "az-cid"

    def test_external_browser_config_receives_workspace_id(self):
        with (
            mock.patch(
                "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
                return_value=True,
            ),
            mock.patch("dbt.adapters.databricks.credentials.Config") as cfg,
        ):
            creds = DatabricksCredentials(
                host="spog.example.com",
                http_path="/sql/1.0/warehouses/abc?o=64",
                client_id="cid",
                database="main",
                schema="default",
            )
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_external_browser()
        kwargs = cfg.call_args.kwargs
        assert kwargs["host"] == "spog.example.com"
        assert kwargs["client_id"] == "cid"
        assert kwargs["workspace_id"] == "64"
        assert kwargs["auth_type"] == "external-browser"
