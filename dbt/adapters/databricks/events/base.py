from abc import ABC

from databricks.sql.exc import Error


class ErrorEvent(ABC):
    def __init__(self, exception: Exception, message: str):
        self.message = message
        self.exception = exception

    def __str__(self) -> str:
        return f"{self.message}: {self.exception}"


class SQLErrorEvent:
    def __init__(self, exception: Exception, message: str):
        self.message = message
        self.exception = exception

    def __str__(self) -> str:
        properties = ""
        if isinstance(self.exception, Error):
            properties = "\nError properties: "
            properties += ", ".join(
                [f"{key}={value}" for key, value in sorted(self.exception.context.items())]
            )

        return f"{self.message}: {self.exception}{properties}"
