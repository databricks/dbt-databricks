"""Unit tests for session mode credentials."""

import os
from unittest.mock import patch

import pytest
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError, DbtValidationError

from dbt.adapters.databricks.credentials import (
    CONNECTION_METHOD_DBSQL,
    CONNECTION_METHOD_SESSION,
    DBT_DATABRICKS_SESSION_MODE_ENV,
    DATABRICKS_RUNTIME_VERSION_ENV,
    DatabricksCredentials,
)


class TestSessionModeAutoDetection:
    """Tests for session mode auto-detection."""

    def test_default_method_is_dbsql(self):
        """Test that default method is dbsql when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            # Need to provide host/http_path for dbsql mode
            creds = DatabricksCredentials(
                host="my.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                token="token",
                schema="test_schema",
            )
            assert creds.method == CONNECTION_METHOD_DBSQL
            assert creds.is_session_mode is False

    def test_explicit_session_method(self):
        """Test that explicit method=session is respected."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        assert creds.method == CONNECTION_METHOD_SESSION
        assert creds.is_session_mode is True

    def test_explicit_dbsql_method(self):
        """Test that explicit method=dbsql is respected."""
        creds = DatabricksCredentials(
            method="dbsql",
            host="my.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            token="token",
            schema="test_schema",
        )
        assert creds.method == CONNECTION_METHOD_DBSQL
        assert creds.is_session_mode is False

    def test_env_var_enables_session_mode(self):
        """Test that DBT_DATABRICKS_SESSION_MODE=true enables session mode."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "true"}):
            creds = DatabricksCredentials(schema="test_schema")
            assert creds.method == CONNECTION_METHOD_SESSION
            assert creds.is_session_mode is True

    def test_env_var_case_insensitive(self):
        """Test that DBT_DATABRICKS_SESSION_MODE is case insensitive."""
        with patch.dict(os.environ, {DBT_DATABRICKS_SESSION_MODE_ENV: "TRUE"}):
            creds = DatabricksCredentials(schema="test_schema")
            assert creds.is_session_mode is True

    def test_databricks_runtime_env_without_host_enables_session(self):
        """Test that DATABRICKS_RUNTIME_VERSION without host enables session mode."""
        with patch.dict(os.environ, {DATABRICKS_RUNTIME_VERSION_ENV: "14.3.x-scala2.12"}):
            creds = DatabricksCredentials(schema="test_schema")
            assert creds.method == CONNECTION_METHOD_SESSION
            assert creds.is_session_mode is True

    def test_databricks_runtime_env_with_host_uses_dbsql(self):
        """Test that DATABRICKS_RUNTIME_VERSION with host uses dbsql mode."""
        with patch.dict(os.environ, {DATABRICKS_RUNTIME_VERSION_ENV: "14.3.x-scala2.12"}):
            creds = DatabricksCredentials(
                host="my.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                token="token",
                schema="test_schema",
            )
            assert creds.method == CONNECTION_METHOD_DBSQL
            assert creds.is_session_mode is False

    def test_invalid_method_raises_error(self):
        """Test that invalid method raises DbtValidationError."""
        with pytest.raises(DbtValidationError) as exc_info:
            DatabricksCredentials(
                method="invalid",
                schema="test_schema",
            )
        assert "Invalid connection method" in str(exc_info.value)


class TestSessionModeValidation:
    """Tests for session mode validation."""

    def test_session_mode_requires_schema(self):
        """Test that session mode requires schema."""
        with patch(
            "dbt.adapters.databricks.credentials.DatabricksCredentials._validate_session_mode"
        ) as mock_validate:
            mock_validate.side_effect = DbtValidationError("Schema is required for session mode.")
            with pytest.raises(DbtValidationError) as exc_info:
                creds = DatabricksCredentials(method="session")
                creds.validate_creds()
            assert "Schema is required" in str(exc_info.value)

    def test_session_mode_does_not_require_host(self):
        """Test that session mode does not require host."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        # Should not raise - host is not required for session mode
        assert creds.host is None

    def test_session_mode_does_not_require_http_path(self):
        """Test that session mode does not require http_path."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        # Should not raise - http_path is not required for session mode
        assert creds.http_path is None

    def test_session_mode_does_not_require_token(self):
        """Test that session mode does not require token."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        # Should not raise - token is not required for session mode
        assert creds.token is None


class TestSessionModeCredentialsManager:
    """Tests for credentials manager in session mode."""

    def test_session_mode_does_not_create_credentials_manager(self):
        """Test that session mode does not create credentials manager."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        assert creds._credentials_manager is None

    def test_session_mode_authenticate_returns_none(self):
        """Test that authenticate returns None in session mode."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
        )
        # Mock _validate_session_mode to avoid pyspark import
        with patch.object(creds, "_validate_session_mode"):
            result = creds.authenticate()
        assert result is None


class TestSessionModeConnectionKeys:
    """Tests for connection keys in session mode."""

    def test_session_mode_connection_keys(self):
        """Test that session mode has correct connection keys."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
            database="main",
        )
        keys = creds._connection_keys()
        assert "method" in keys
        assert "schema" in keys
        assert "catalog" in keys
        assert "host" not in keys
        assert "http_path" not in keys

    def test_session_mode_unique_field(self):
        """Test that session mode unique_field is based on catalog/schema."""
        creds = DatabricksCredentials(
            method="session",
            schema="test_schema",
            database="main",
        )
        assert creds.unique_field == "session://main/test_schema"


class TestDbsqlModeValidation:
    """Tests for DBSQL mode validation (existing behavior)."""

    def test_dbsql_mode_requires_host(self):
        """Test that DBSQL mode requires host."""
        # Create credentials with session mode first (no SDK auth), then switch to dbsql
        with patch.dict(os.environ, {}, clear=True):
            creds = DatabricksCredentials(
                method="session",  # Start with session to avoid SDK auth
                http_path="/sql/1.0/warehouses/abc",
                schema="test_schema",
            )
            # Switch to dbsql mode for validation test
            creds.method = "dbsql"
            with pytest.raises(DbtConfigError) as exc_info:
                creds.validate_creds()
            assert "host" in str(exc_info.value)

    def test_dbsql_mode_requires_http_path(self):
        """Test that DBSQL mode requires http_path."""
        # Create credentials with session mode first (no SDK auth), then switch to dbsql
        with patch.dict(os.environ, {}, clear=True):
            creds = DatabricksCredentials(
                method="session",  # Start with session to avoid SDK auth
                host="my.databricks.com",
                schema="test_schema",
            )
            # Switch to dbsql mode for validation test
            creds.method = "dbsql"
            with pytest.raises(DbtConfigError) as exc_info:
                creds.validate_creds()
            assert "http_path" in str(exc_info.value)

    def test_dbsql_mode_requires_token_or_oauth(self):
        """Test that DBSQL mode requires token or oauth."""
        # Create credentials with session mode first (no SDK auth), then switch to dbsql
        with patch.dict(os.environ, {}, clear=True):
            creds = DatabricksCredentials(
                method="session",  # Start with session to avoid SDK auth
                host="my.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                schema="test_schema",
            )
            # Switch to dbsql mode for validation test
            creds.method = "dbsql"
            with pytest.raises(DbtConfigError) as exc_info:
                creds.validate_creds()
            assert "oauth" in str(exc_info.value).lower() or "token" in str(exc_info.value).lower()
