from dbt.adapters.databricks.events.base import ErrorEvent


class CredentialLoadError(ErrorEvent):
    def __init__(self, exception: Exception):
        super().__init__(exception, "Exception while trying to load credentials")


class CredentialSaveError(ErrorEvent):
    def __init__(self, exception: Exception):
        super().__init__(exception, "Exception while trying to save credentials")


class CredentialShardEvent:
    def __init__(self, password_len: int):
        self.password_len = password_len

    def __str__(self) -> str:
        return f"Password is {self.password_len} characters, sharding it"
