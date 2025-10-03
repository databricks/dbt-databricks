from dbt.adapters.databricks.api_client import is_retryable_api_error


class TestRetryLogic:
    """Test the retry logic helper functions."""

    def test_is_retryable_api_error__timeout_errors(self):
        """Test that timeout errors are detected as retryable."""
        assert is_retryable_api_error(Exception("Request timed out"))
        assert is_retryable_api_error(Exception("Connection timeout"))
        assert is_retryable_api_error(Exception("Operation timeout"))

    def test_is_retryable_api_error__connection_errors(self):
        """Test that connection errors are detected as retryable."""
        assert is_retryable_api_error(Exception("Connection refused"))
        assert is_retryable_api_error(Exception("Connection error"))
        assert is_retryable_api_error(Exception("Connection reset"))

    def test_is_retryable_api_error__rate_limiting(self):
        """Test that rate limiting errors are detected as retryable."""
        assert is_retryable_api_error(Exception("Rate limit exceeded"))
        assert is_retryable_api_error(Exception("Too many requests"))
        assert is_retryable_api_error(Exception("Throttle limit reached"))

    def test_is_retryable_api_error__server_errors(self):
        """Test that temporary server errors are detected as retryable."""
        assert is_retryable_api_error(Exception("503 Service Unavailable"))
        assert is_retryable_api_error(Exception("502 Bad Gateway"))
        assert is_retryable_api_error(Exception("504 Gateway Timeout"))
        assert is_retryable_api_error(Exception("Temporarily unavailable"))
        assert is_retryable_api_error(Exception("Resource exhausted"))
        assert is_retryable_api_error(Exception("Deadline exceeded"))

    def test_is_retryable_api_error__non_retryable(self):
        """Test that non-retryable errors are correctly identified."""
        assert not is_retryable_api_error(Exception("Invalid credentials"))
        assert not is_retryable_api_error(Exception("Not found"))
        assert not is_retryable_api_error(Exception("Permission denied"))
        assert not is_retryable_api_error(Exception("Invalid request"))
        assert not is_retryable_api_error(Exception("Validation error"))

    def test_is_retryable_api_error__case_insensitive(self):
        """Test that error detection is case-insensitive."""
        assert is_retryable_api_error(Exception("TIMEOUT"))
        assert is_retryable_api_error(Exception("Timeout"))
        assert is_retryable_api_error(Exception("RATE LIMIT"))
        assert is_retryable_api_error(Exception("Rate Limit"))

    def test_is_retryable_api_error__programming_errors_not_retried(self):
        """Test that programming errors are never retried."""
        assert not is_retryable_api_error(
            AttributeError("'NoneType' object has no attribute 'foo'")
        )
        assert not is_retryable_api_error(TypeError("expected str, got int"))
        assert not is_retryable_api_error(ValueError("invalid literal for int()"))
        assert not is_retryable_api_error(KeyError("key not found"))
        assert not is_retryable_api_error(KeyboardInterrupt())

    def test_is_retryable_api_error__specific_transient_errors(self):
        """Test specific transient error patterns that should be retried."""
        # HTTP status codes
        assert is_retryable_api_error(Exception("HTTP 429: Too Many Requests"))
        assert is_retryable_api_error(Exception("502 Bad Gateway"))
        assert is_retryable_api_error(Exception("Service returned 503"))

        # Connection issues
        assert is_retryable_api_error(Exception("Connection reset by peer"))
        assert is_retryable_api_error(Exception("Connection refused"))

        # Resource issues
        assert is_retryable_api_error(Exception("Resource exhausted"))
        assert is_retryable_api_error(Exception("Deadline exceeded while processing"))
