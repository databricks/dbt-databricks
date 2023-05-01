from typing import Any, Dict, Optional
from databricks.sdk.oauth import ClientCredentials, Token, TokenSource
from databricks.sdk.core import CredentialsProvider, HeaderFactory, Config, credentials_provider


class token_auth(CredentialsProvider):
    _token: str

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_type(self) -> str:
        return "token"

    def as_dict(self) -> dict:
        return {"token": self._token}

    @staticmethod
    def from_dict(raw: Optional[dict]) -> CredentialsProvider:
        if not raw:
            return None
        return token_auth(raw["token"])

    def __call__(self, *args: tuple, **kwargs: Dict[str, Any]) -> HeaderFactory:
        static_credentials = {"Authorization": f"Bearer {self._token}"}

        def inner() -> Dict[str, str]:
            return static_credentials

        return inner


class m2m_auth(CredentialsProvider):
    _token_source: TokenSource = None

    def __init__(self, host: str, client_id: str, client_secret: str) -> None:
        @credentials_provider("noop", [])
        def noop_credentials(_: Any):  # type: ignore
            return lambda: {}

        config = Config(host=host, credentials_provider=noop_credentials)
        oidc = config.oidc_endpoints
        scopes = ["offline_access", "all-apis"]
        if not oidc:
            raise ValueError(f"{host} does not support OAuth")
        if config.is_azure:
            # Azure AD only supports full access to Azure Databricks.
            scopes = [f"{config.effective_azure_login_app_id}/.default", "offline_access"]
        self._token_source = ClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
            token_url=oidc.token_endpoint,
            scopes=scopes,
            use_header="microsoft" not in oidc.token_endpoint,
            use_params="microsoft" in oidc.token_endpoint,
        )

    def auth_type(self) -> str:
        return "oauth"

    def as_dict(self) -> dict:
        if self._token_source:
            return {"token": self._token_source.token().as_dict()}
        else:
            return {"token": {}}

    @staticmethod
    def from_dict(host: str, client_id: str, client_secret: str, raw: dict) -> CredentialsProvider:
        c = m2m_auth(host=host, client_id=client_id, client_secret=client_secret)
        c._token_source._token = Token.from_dict(raw["token"])
        return c

    def __call__(self, *args: tuple, **kwargs: Dict[str, Any]) -> HeaderFactory:
        def inner() -> Dict[str, str]:
            token = self._token_source.token()
            return {"Authorization": f"{token.token_type} {token.access_token}"}

        return inner
