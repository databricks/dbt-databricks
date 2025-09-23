import itertools
import json
import os
import re
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Optional, Union, cast

import keyring
from dbt_common.exceptions import DbtConfigError, DbtValidationError

from databricks.sdk.core import CredentialsProvider
from databricks.sdk.oauth import OAuthClient, SessionCredentials
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.databricks.auth import m2m_auth, token_auth
from dbt.adapters.databricks.events.credential_events import (
    CredentialLoadError,
    CredentialSaveError,
    CredentialShardEvent,
)
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.logging import logger

CATALOG_KEY_IN_SESSION_PROPERTIES = "databricks.catalog"
DBT_DATABRICKS_INVOCATION_ENV_REGEX = re.compile("^[A-z0-9\\-]+$")
EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/(.*)")
DBT_DATABRICKS_HTTP_SESSION_HEADERS = "DBT_DATABRICKS_HTTP_SESSION_HEADERS"

REDIRECT_URL = "http://localhost:8020"
CLIENT_ID = "dbt-databricks"
SCOPES = ["all-apis", "offline_access"]
MAX_NT_PASSWORD_SIZE = 1280

# When using an Azure App Registration with the SPA platform, the refresh token will
# also expire after 24h. Silently accept this in this case.
SPA_CLIENT_FIXED_TIME_LIMIT_ERROR = "AADSTS700084"

TCredentialProvider = Union[CredentialsProvider, SessionCredentials]


@dataclass
class DatabricksCredentials(Credentials):
    database: Optional[str] = None  # type: ignore[assignment]
    schema: Optional[str] = None  # type: ignore[assignment]
    host: Optional[str] = None
    http_path: Optional[str] = None
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    oauth_redirect_url: Optional[str] = None
    oauth_scopes: Optional[list[str]] = None
    session_properties: Optional[dict[str, Any]] = None
    connection_parameters: Optional[dict[str, Any]] = None
    auth_type: Optional[str] = None

    # Named compute resources specified in the profile. Used for
    # creating a connection when a model specifies a compute resource.
    compute: Optional[dict[str, Any]] = None

    connect_retries: int = 1
    connect_timeout: Optional[int] = None
    retry_all: bool = False
    connect_max_idle: Optional[int] = None

    _credentials_provider: Optional[dict[str, Any]] = None
    _lock = threading.Lock()  # to avoid concurrent auth

    _ALIASES = {
        "catalog": "database",
        "target_catalog": "target_database",
    }

    @classmethod
    def __pre_deserialize__(cls, data: dict[Any, Any]) -> dict[Any, Any]:
        data = super().__pre_deserialize__(data)
        data.setdefault("database", None)
        data.setdefault("connection_parameters", {})
        data["connection_parameters"].setdefault("_retry_stop_after_attempts_count", 30)
        data["connection_parameters"].setdefault("_retry_delay_max", 60)
        return data

    def __post_init__(self) -> None:
        if "." in (self.schema or ""):
            raise DbtValidationError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use `catalog` instead.\n"
            )

        session_properties = self.session_properties or {}
        if CATALOG_KEY_IN_SESSION_PROPERTIES in session_properties:
            if self.database is None:
                self.database = session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
                del session_properties[CATALOG_KEY_IN_SESSION_PROPERTIES]
            else:
                raise DbtValidationError(
                    f"Got duplicate keys: (`{CATALOG_KEY_IN_SESSION_PROPERTIES}` "
                    'in session_properties) all map to "database"'
                )
        self.session_properties = session_properties

        if self.database is not None:
            database = self.database.strip()
            if not database:
                raise DbtValidationError(f"Invalid catalog name : `{self.database}`.")
            self.database = database
        else:
            self.database = "hive_metastore"

        connection_parameters = self.connection_parameters or {}
        for key in (
            "server_hostname",
            "http_path",
            "access_token",
            "client_id",
            "client_secret",
            "session_configuration",
            "catalog",
            "schema",
            "_user_agent_entry",
        ):
            if key in connection_parameters:
                raise DbtValidationError(f"The connection parameter `{key}` is reserved.")
        if "http_headers" in connection_parameters:
            http_headers = connection_parameters["http_headers"]
            if not isinstance(http_headers, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in http_headers.items()
            ):
                raise DbtValidationError(
                    "The connection parameter `http_headers` should be dict of strings: "
                    f"{http_headers}."
                )
        if "_socket_timeout" not in connection_parameters:
            connection_parameters["_socket_timeout"] = 600
        self.connection_parameters = connection_parameters

    def validate_creds(self) -> None:
        for key in ["host", "http_path"]:
            if not getattr(self, key):
                raise DbtConfigError(f"The config '{key}' is required to connect to Databricks")
        if not self.token and self.auth_type != "oauth":
            raise DbtConfigError(
                "The config `auth_type: oauth` is required when not using access token"
            )

        if not self.client_id and self.client_secret:
            raise DbtConfigError(
                "The config 'client_id' is required to connect "
                "to Databricks when 'client_secret' is present"
            )

    @classmethod
    def get_invocation_env(cls) -> Optional[str]:
        invocation_env = GlobalState.get_invocation_env()
        if invocation_env:
            # Thrift doesn't allow nested () so we need to ensure
            # that the passed user agent is valid.
            if not DBT_DATABRICKS_INVOCATION_ENV_REGEX.search(invocation_env):
                raise DbtValidationError(f"Invalid invocation environment: {invocation_env}")
        return invocation_env

    @classmethod
    def get_all_http_headers(cls, user_http_session_headers: dict[str, str]) -> dict[str, str]:
        http_session_headers_str = GlobalState.get_http_session_headers()

        http_session_headers_dict: dict[str, str] = (
            {
                k: v if isinstance(v, str) else json.dumps(v)
                for k, v in json.loads(http_session_headers_str).items()
            }
            if http_session_headers_str is not None
            else {}
        )

        intersect_http_header_keys = (
            user_http_session_headers.keys() & http_session_headers_dict.keys()
        )

        if len(intersect_http_header_keys) > 0:
            raise DbtValidationError(
                f"Intersection with reserved http_headers in keys: {intersect_http_header_keys}"
            )

        http_session_headers_dict.update(user_http_session_headers)

        return http_session_headers_dict

    @property
    def type(self) -> str:
        return "databricks"

    @property
    def unique_field(self) -> str:
        return cast(str, self.host)

    def connection_info(self, *, with_aliases: bool = False) -> Iterable[tuple[str, Any]]:
        as_dict = self.to_dict(omit_none=False)
        connection_keys = set(self._connection_keys(with_aliases=with_aliases))
        aliases: list[str] = []
        if with_aliases:
            aliases = [k for k, v in self._ALIASES.items() if v in connection_keys]
        for key in itertools.chain(self._connection_keys(with_aliases=with_aliases), aliases):
            if key in as_dict:
                yield key, as_dict[key]

    def _connection_keys(self, *, with_aliases: bool = False) -> tuple[str, ...]:
        # Assuming `DatabricksCredentials.connection_info(self, *, with_aliases: bool = False)`
        # is called from only:
        #
        # - `Profile` with `with_aliases=True`
        # - `DebugTask` without `with_aliases` (`False` by default)
        #
        # Thus, if `with_aliases` is `True`, `DatabricksCredentials._connection_keys` should return
        # the internal key names; otherwise it can use aliases to show in `dbt debug`.
        connection_keys = ["host", "http_path", "schema"]
        if with_aliases:
            connection_keys.insert(2, "database")
        elif self.database:
            connection_keys.insert(2, "catalog")
        if self.session_properties:
            connection_keys.append("session_properties")
        return tuple(connection_keys)

    @classmethod
    def extract_cluster_id(cls, http_path: str) -> Optional[str]:
        m = EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX.match(http_path)
        if m:
            return m.group(1).strip()
        else:
            return None

    @property
    def cluster_id(self) -> Optional[str]:
        return self.extract_cluster_id(self.http_path)  # type: ignore[arg-type]

    def authenticate(self, in_provider: Optional[TCredentialProvider]) -> TCredentialProvider:
        self.validate_creds()
        host: str = self.host or ""
        if self._credentials_provider:
            return self._provider_from_dict()  # type: ignore
        if in_provider:
            if isinstance(in_provider, m2m_auth) or isinstance(in_provider, token_auth):
                self._credentials_provider = in_provider.as_dict()
            return in_provider

        provider: TCredentialProvider
        # dbt will spin up multiple threads. This has to be sync. So lock here
        self._lock.acquire()
        try:
            if self.token:
                provider = token_auth(self.token)
                self._credentials_provider = provider.as_dict()
                return provider

            if self.client_id and self.client_secret:
                provider = m2m_auth(
                    host=host,
                    client_id=self.client_id or "",
                    client_secret=self.client_secret or "",
                )
                self._credentials_provider = provider.as_dict()
                return provider

            client_id = self.client_id or CLIENT_ID
            redirect_url = self.oauth_redirect_url or REDIRECT_URL
            scopes = self.oauth_scopes or SCOPES

            oauth_client = OAuthClient(
                host=host,
                client_id=client_id,
                client_secret="",
                redirect_url=redirect_url,
                scopes=scopes,
            )
            # optional branch. Try and keep going if it does not work
            try:
                # try to get cached credentials
                credsdict = self.get_sharded_password("dbt-databricks", host)

                if credsdict:
                    provider = SessionCredentials.from_dict(oauth_client, json.loads(credsdict))
                    # if refresh token is expired, this will throw
                    try:
                        if provider.token().valid:
                            self._credentials_provider = provider.as_dict()
                            if json.loads(credsdict) != provider.as_dict():
                                # if the provider dict has changed, most likely because of a token
                                # refresh, save it for further use
                                self.set_sharded_password(
                                    "dbt-databricks", host, json.dumps(self._credentials_provider)
                                )
                            return provider
                    except Exception as e:
                        # SPA token are supposed to expire after 24h, no need to warn
                        if SPA_CLIENT_FIXED_TIME_LIMIT_ERROR in str(e):
                            logger.debug(CredentialLoadError(e))
                        else:
                            logger.warning(CredentialLoadError(e))
                        # whatever it is, get rid of the cache
                        self.delete_sharded_password("dbt-databricks", host)

            # error with keyring. Maybe machine has no password persistency
            except Exception as e:
                logger.warning(CredentialLoadError(e))

            # no token, go fetch one
            consent = oauth_client.initiate_consent()

            provider = consent.launch_external_browser()
            # save for later
            self._credentials_provider = provider.as_dict()
            try:
                self.set_sharded_password(
                    "dbt-databricks", host, json.dumps(self._credentials_provider)
                )
            # error with keyring. Maybe machine has no password persistency
            except Exception as e:
                logger.warning(CredentialSaveError(e))

            return provider

        finally:
            self._lock.release()

    def set_sharded_password(self, service_name: str, username: str, password: str) -> None:
        max_size = MAX_NT_PASSWORD_SIZE

        # if not Windows or "small" password, stick to the default
        if os.name != "nt" or len(password) < max_size:
            keyring.set_password(service_name, username, password)
        else:
            logger.debug(CredentialShardEvent(len(password)))

            password_shards = [
                password[i : i + max_size] for i in range(0, len(password), max_size)
            ]
            shard_info = {
                "sharded_password": True,
                "shard_count": len(password_shards),
            }

            # store the "shard info" as the "base" password
            keyring.set_password(service_name, username, json.dumps(shard_info))
            # then store all shards with the shard number as postfix
            for i, s in enumerate(password_shards):
                keyring.set_password(service_name, f"{username}__{i}", s)

    def get_sharded_password(self, service_name: str, username: str) -> Optional[str]:
        password = keyring.get_password(service_name, username)

        # check for "shard info" stored as json
        try:
            password_as_dict = json.loads(str(password))
            if password_as_dict.get("sharded_password"):
                # if password was stored shared, reconstruct it
                shard_count = int(password_as_dict.get("shard_count"))

                password = ""
                for i in range(shard_count):
                    password += str(keyring.get_password(service_name, f"{username}__{i}"))
        except ValueError:
            pass

        return password

    def delete_sharded_password(self, service_name: str, username: str) -> None:
        password = keyring.get_password(service_name, username)

        # check for "shard info" stored as json. If so delete all shards
        try:
            password_as_dict = json.loads(str(password))
            if password_as_dict.get("sharded_password"):
                shard_count = int(password_as_dict.get("shard_count"))
                for i in range(shard_count):
                    keyring.delete_password(service_name, f"{username}__{i}")
        except ValueError:
            pass

        # delete "base" password
        keyring.delete_password(service_name, username)

    def _provider_from_dict(self) -> Optional[TCredentialProvider]:
        if self.token:
            return token_auth.from_dict(self._credentials_provider)

        if self.client_id and self.client_secret:
            return m2m_auth.from_dict(
                host=self.host or "",
                client_id=self.client_id or "",
                client_secret=self.client_secret or "",
                raw=self._credentials_provider or {"token": {}},
            )

        oauth_client = OAuthClient(
            host=self.host or "",
            client_id=self.client_id or CLIENT_ID,
            client_secret="",
            redirect_url=self.oauth_redirect_url or REDIRECT_URL,
            scopes=self.oauth_scopes or SCOPES,
        )

        return SessionCredentials.from_dict(
            client=oauth_client, raw=self._credentials_provider or {"token": {}}
        )
