from typing import Any
from typing import Dict
from typing import Optional

from databricks.sdk.core import Config
from databricks.sdk.core import credentials_provider
from databricks.sdk.core import CredentialsProvider
from databricks.sdk.core import HeaderFactory
from databricks.sdk.oauth import ClientCredentials
from databricks.sdk.oauth import Token
from databricks.sdk.oauth import TokenSource
from requests import PreparedRequest
from requests.auth import AuthBase


class token_auth(CredentialsProvider):
    _token: str

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_type(self) -> str:
        return "token"

    def as_dict(self) -> dict:
        return {"token": self._token}

    @staticmethod
    def from_dict(raw: Optional[dict]) -> Optional[CredentialsProvider]:
        if not raw:
            return None
        return token_auth(raw["token"])

    def __call__(self, _: Optional[Config] = None) -> HeaderFactory:
        static_credentials = {"Authorization": f"Bearer {self._token}"}

        def inner() -> Dict[str, str]:
            return static_credentials

        return inner


class m2m_auth(CredentialsProvider):
    _token_source: Optional[TokenSource] = None

    def __init__(self, host: str, client_id: str, client_secret: str) -> None:
        @credentials_provider("noop", [])
        def noop_credentials(_: Any):  # type: ignore
            return lambda: {}

        config = Config(host=host, credentials_provider=noop_credentials)
        oidc = config.oidc_endpoints
        scopes = ["all-apis"]
        if not oidc:
            raise ValueError(f"{host} does not support OAuth")
        if config.is_azure:
            # Azure AD only supports full access to Azure Databricks.
            scopes = [f"{config.effective_azure_login_app_id}/.default"]
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
        c._token_source._token = Token.from_dict(raw["token"])  # type: ignore
        return c

    def __call__(self, _: Optional[Config] = None) -> HeaderFactory:
        def inner() -> Dict[str, str]:
            token = self._token_source.token()  # type: ignore
            return {"Authorization": f"{token.token_type} {token.access_token}"}

        return inner


class BearerAuth(AuthBase):
    """This mix-in is passed to our requests Session to explicitly
    use the bearer authentication method.

    Without this, a local .netrc file in the user's home directory
    will override the auth headers provided by our header_factory.

    More details in issue #337.
    """

    def __init__(self, header_factory: HeaderFactory):
        self.header_factory = header_factory

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers.update(**self.header_factory())
        return r
