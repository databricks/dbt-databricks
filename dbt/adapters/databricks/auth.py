from typing import Dict
from databricks.sdk.oauth import OAuthClient, ClientCredentials, Token, RefreshableCredentials
from databricks.sdk.core import CredentialsProvider, HeaderFactory

import requests

REDIRECT_URL = "http://localhost:8050"
CLIENT_ID = "dbt-databricks"
SCOPES = ['all-apis', 'offline_access']

def authenticate(creds) -> CredentialsProvider:
    if creds.token:
        return token_auth(creds.token)
    
    if creds.client_id and creds.client_secret:
        return m2m_auth(creds.host, creds.client_id, creds.client_secret)
    
    if (creds.client_id and not creds.client_secret) or (not creds.client_id and creds.client_secret):
        raise "missing credentials"
    
    oauth_client = OAuthClient(host=creds.host,
                           client_id=CLIENT_ID, 
                           client_secret=None,
                           redirect_url=REDIRECT_URL,
                           scopes=SCOPES)
    
    consent = oauth_client.initiate_consent()

    return consent.launch_external_browser()

def from_dict(creds, raw) -> CredentialsProvider:
    if creds.token:
        return token_auth.from_dict(raw)
    
    if creds.client_id and creds.client_secret:
        return m2m_auth.from_dict(host=creds.host, client_id=creds.client_id, client_secret=creds.client_secret, raw=raw)
    
    if (creds.client_id and not creds.client_secret) or (not creds.client_id and creds.client_secret):
        raise "missing credentials"
    
    oauth_client = OAuthClient(host=creds.host,
                        client_id=CLIENT_ID,
                        client_secret=None,
                        redirect_url=REDIRECT_URL,
                        scopes=SCOPES)
    
    return RefreshableCredentials.from_dict(client=oauth_client, raw=raw)
    

class token_auth(CredentialsProvider):
    _token: str
    
    def __init__(self, token) -> None:
        self._token = token
    
    def auth_type(self) -> str:
        return "token"
    
    def as_dict(self) -> dict:
        return {'token': self._token}
    
    @staticmethod
    def from_dict(raw: dict) -> CredentialsProvider:
        return token_auth(raw["token"])

    def __call__(self, *args, **kwargs) -> HeaderFactory:
        static_credentials = {'Authorization': f'Bearer {self._token}'}

        def inner() -> Dict[str, str]:
            return static_credentials
        return inner

    
class m2m_auth(CredentialsProvider):
    _token_source = None
    
    def __init__(self, host: str, client_id: str, client_secret: str) -> None:
        resp = requests.get(f"https://{host}/oidc/.well-known/oauth-authorization-server")
        if not resp.ok:
            return None
        self._token_source = ClientCredentials(client_id=client_id,
                                               client_secret=client_secret,
                                               token_url=resp.json()["token_endpoint"],
                                               scopes=SCOPES,
                                               use_header=True)

    def auth_type(self) -> str:
        return "oauth"
    
    def as_dict(self) -> dict:
        if self._token_source:
            return {'token': self._token_source.token().as_dict()}
        else:
            return {'token': {}}
    
    @staticmethod
    def from_dict(host: str, client_id: str, client_secret: str, raw: dict) -> CredentialsProvider:
        c = m2m_auth(host=host, client_id=client_id, client_secret=client_secret)
        c._token_source._token = Token.from_dict(raw["token"])
        return c

    
    def __call__(self, *args, **kwargs) -> HeaderFactory:

        def inner() -> Dict[str, str]:
            token = self._token_source.token()
            return {'Authorization': f'{token.token_type} {token.access_token}'}

        return inner