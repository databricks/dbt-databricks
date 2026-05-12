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


class TestValidateCreds:
    BASE = dict(host="my.cloud.databricks.com", http_path="/sql/1.0/warehouses/abc")

    def _creds(self, **kwargs):
        # Mock Config so __post_init__ doesn't make real network calls.
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            return DatabricksCredentials(database="db", schema="sch", **self.BASE, **kwargs)

    def test_token_still_valid(self):
        self._creds(token="mytoken").validate_creds()

    def test_oauth_auth_type_still_valid(self):
        self._creds(auth_type="oauth").validate_creds()

    def test_azure_cli_auth_type_valid(self):
        """auth_type values other than 'oauth' are now accepted without a token."""
        self._creds(auth_type="azure-cli").validate_creds()

    def test_azure_msi_auth_type_valid(self):
        self._creds(auth_type="azure-msi").validate_creds()

    def test_databricks_cli_auth_type_valid(self):
        self._creds(auth_type="databricks-cli").validate_creds()

    def test_google_credentials_auth_type_valid(self):
        self._creds(auth_type="google-credentials").validate_creds()

    def test_metadata_service_auth_type_valid(self):
        self._creds(auth_type="metadata-service").validate_creds()

    def test_host_required(self):
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            with pytest.raises(DbtConfigError, match="host"):
                DatabricksCredentials(
                    database="db", schema="sch", http_path="/sql/1.0/warehouses/abc", token="t"
                ).validate_creds()

    def test_http_path_required(self):
        with patch("dbt.adapters.databricks.credentials.Config") as mc:
            mc.return_value = MagicMock()
            with pytest.raises(DbtConfigError, match="http_path"):
                DatabricksCredentials(
                    database="db", schema="sch", host="my.cloud.databricks.com", token="t"
                ).validate_creds()

    def test_client_id_required_when_client_secret_present(self):
        with pytest.raises(DbtConfigError, match="client_id"):
            self._creds(auth_type="oauth", client_secret="secret").validate_creds()

    def test_azure_credentials_must_be_paired(self):
        with pytest.raises(DbtConfigError, match="azure_client"):
            self._creds(token="t", azure_client_id="id").validate_creds()

    def test_client_secret_and_azure_client_secret_are_mutually_exclusive(self):
        with pytest.raises(DbtConfigError, match="mutually exclusive"):
            self._creds(
                client_id="my-client",
                client_secret="my-secret",
                azure_client_id="az-client",
                azure_client_secret="az-secret",
            ).validate_creds()

    def test_bare_profile_warns_about_ambient_discovery(self):
        """A profile with no auth fields should warn that SDK ambient discovery is used."""
        import logging

        creds = self._creds()
        with patch("dbt.adapters.databricks.credentials.logger") as mock_logger:
            creds.validate_creds()
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("ambient" in c for c in warning_calls), (
                f"Expected ambient-discovery warning, got: {warning_calls}"
            )

    def test_token_profile_does_not_warn(self):
        """A profile with a token should not trigger the ambient-discovery warning."""
        creds = self._creds(token="mytoken")
        with patch("dbt.adapters.databricks.credentials.logger") as mock_logger:
            creds.validate_creds()
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert not any("ambient" in c for c in warning_calls)


class TestSdkAuthTypePassthrough:
    """Verify that explicit auth_type values are forwarded to the Databricks SDK Config."""

    BASE = dict(host="my.cloud.databricks.com", http_path="/sql/1.0/warehouses/abc")

    def _make_manager(self, auth_type: str, **kwargs):
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            creds = DatabricksCredentials(
                database="db", schema="sch", auth_type=auth_type, **self.BASE, **kwargs
            )
            return creds._credentials_manager, mock_config

    def test_azure_cli_passed_to_sdk(self):
        manager, mock_config = self._make_manager("azure-cli")
        mock_config.assert_called_once_with(host="my.cloud.databricks.com", auth_type="azure-cli")

    def test_azure_msi_passed_to_sdk(self):
        manager, mock_config = self._make_manager("azure-msi")
        mock_config.assert_called_once_with(host="my.cloud.databricks.com", auth_type="azure-msi")

    def test_azure_msi_with_user_assigned_identity(self):
        """azure_client_id should be forwarded for user-assigned managed identities."""
        manager, mock_config = self._make_manager("azure-msi", azure_client_id="my-msi-client-id")
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com",
            auth_type="azure-msi",
            azure_client_id="my-msi-client-id",
        )

    def test_azure_msi_with_tenant(self):
        """azure_tenant_id should be forwarded when set."""
        manager, mock_config = self._make_manager("azure-msi", azure_tenant_id="my-tenant")
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com",
            auth_type="azure-msi",
            azure_tenant_id="my-tenant",
        )

    def test_databricks_cli_passed_to_sdk(self):
        manager, mock_config = self._make_manager("databricks-cli")
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com", auth_type="databricks-cli"
        )

    def test_databricks_cli_with_profile(self):
        """databricks_cli_profile maps to the SDK's 'profile' kwarg."""
        manager, mock_config = self._make_manager(
            "databricks-cli", databricks_cli_profile="my-profile"
        )
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com",
            auth_type="databricks-cli",
            profile="my-profile",
        )

    def test_metadata_service_passed_to_sdk(self):
        manager, mock_config = self._make_manager("metadata-service")
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com", auth_type="metadata-service"
        )

    def test_explicit_oauth_m2m_forwards_credentials(self):
        """When auth_type=oauth-m2m is set explicitly, client_id and client_secret are forwarded."""
        manager, mock_config = self._make_manager(
            "oauth-m2m",
            client_id="my-client-id",
            client_secret="my-client-secret",
        )
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com",
            auth_type="oauth-m2m",
            client_id="my-client-id",
            client_secret="my-client-secret",
        )

    def test_google_credentials_forwarded(self):
        manager, mock_config = self._make_manager(
            "google-credentials", google_service_account="sa@project.iam.gserviceaccount.com"
        )
        mock_config.assert_called_once_with(
            host="my.cloud.databricks.com",
            auth_type="google-credentials",
            google_service_account="sa@project.iam.gserviceaccount.com",
        )

    def test_databricks_sdk_parameters_forwarded(self):
        """Extra params in databricks_sdk_parameters are merged into Config kwargs."""
        manager, mock_config = self._make_manager(
            "azure-cli", databricks_sdk_parameters={"azure_environment": "usgovernment"}
        )
        call_kwargs = mock_config.call_args.kwargs
        assert call_kwargs.get("azure_environment") == "usgovernment"

    def test_databricks_sdk_parameters_override_warns(self):
        """databricks_sdk_parameters that shadows a first-class field should log a warning."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            with patch("dbt.adapters.databricks.credentials.logger") as mock_logger:
                DatabricksCredentials(
                    host="my.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/abc",
                    database="db",
                    schema="sch",
                    token="mytoken",
                    databricks_sdk_parameters={"token": "override-token"},
                )
                warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
                assert any("databricks_sdk_parameters" in c and "token" in c for c in warning_calls), (
                    f"Expected override warning, got: {warning_calls}"
                )

    def test_databricks_sdk_parameters_no_warning_for_new_fields(self):
        """databricks_sdk_parameters with novel fields (not in explicit kwargs) should not warn."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            with patch("dbt.adapters.databricks.credentials.logger") as mock_logger:
                DatabricksCredentials(
                    host="my.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/abc",
                    database="db",
                    schema="sch",
                    auth_type="azure-cli",
                    databricks_sdk_parameters={"some_future_sdk_field": "value"},
                )
                warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
                assert not any("databricks_sdk_parameters" in c for c in warning_calls)

    def test_oauth_still_uses_external_browser(self):
        """'oauth' is a dbt alias for external-browser — backward compat must be preserved."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
                auth_type="oauth",
            )
            call_kwargs = mock_config.call_args.kwargs
            assert call_kwargs.get("auth_type") == "external-browser"
            assert call_kwargs.get("client_id") == CLIENT_ID

    def test_token_forwarded_to_sdk(self):
        """Token is forwarded to the SDK; no auth_type is added when none is set."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
                token="mytoken",
            )
            call_kwargs = mock_config.call_args.kwargs
            assert call_kwargs.get("token") == "mytoken"
            assert "auth_type" not in call_kwargs

    def test_no_credentials_delegates_to_sdk(self):
        """No credentials in profile -> pure SDK delegation for ambient auth discovery."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
            )
            mock_config.assert_called_once_with(host="my.cloud.databricks.com")

    def test_client_id_without_secret_uses_external_browser(self):
        """client_id with no client_secret and no auth_type infers external-browser."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
                client_id="my-custom-client-id",
            )
            call_kwargs = mock_config.call_args.kwargs
            assert call_kwargs.get("auth_type") == "external-browser"
            assert call_kwargs.get("client_id") == "my-custom-client-id"

    def test_azure_sp_infers_auth_type(self):
        """azure_client_id + azure_client_secret without auth_type -> azure-client-secret."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
                azure_client_id="my-azure-client-id",
                azure_client_secret="my-azure-secret",
            )
            call_kwargs = mock_config.call_args.kwargs
            assert call_kwargs.get("auth_type") == "azure-client-secret"
            assert call_kwargs.get("azure_client_id") == "my-azure-client-id"
            assert call_kwargs.get("azure_client_secret") == "my-azure-secret"

    def test_explicit_auth_type_overrides_azure_sp_inference(self):
        """Explicit auth_type wins even when azure_client_id + azure_client_secret are set."""
        manager, mock_config = self._make_manager(
            "azure-msi",
            azure_client_id="my-msi-client-id",
            azure_client_secret="my-secret",
        )
        call_kwargs = mock_config.call_args.kwargs
        assert call_kwargs.get("auth_type") == "azure-msi"
        assert call_kwargs.get("azure_client_id") == "my-msi-client-id"

    def test_oauth_scopes_forwarded_as_sdk_scopes(self):
        """oauth_scopes profile field maps to the SDK Config 'scopes' kwarg."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            DatabricksCredentials(
                host="my.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                database="db",
                schema="sch",
                auth_type="oauth",
                oauth_scopes=["all-apis", "offline_access"],
            )
            call_kwargs = mock_config.call_args.kwargs
            assert call_kwargs.get("scopes") == ["all-apis", "offline_access"]

    def test_oauth_redirect_url_logs_warning(self):
        """oauth_redirect_url is unsupported by the SDK and should trigger a warning."""
        with patch("dbt.adapters.databricks.credentials.Config") as mock_config:
            mock_config.return_value = MagicMock()
            with patch("dbt.adapters.databricks.credentials.logger") as mock_logger:
                DatabricksCredentials(
                    host="my.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/abc",
                    database="db",
                    schema="sch",
                    token="mytoken",
                    oauth_redirect_url="http://localhost:9999",
                )
                warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
                assert any("oauth_redirect_url" in c for c in warning_calls), (
                    f"Expected oauth_redirect_url warning, got: {warning_calls}"
                )
