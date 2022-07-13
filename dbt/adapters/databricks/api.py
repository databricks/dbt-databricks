from databricks_cli.sdk import ApiClient
import databricks_cli.sdk.service as services


class DatabricksAPI:
    def __init__(self, host, token):
        self._api_client = None
        self.host = host
        self.token = token

        if not self.host.startswith("https://"):
            self.host = f"https://{self.host}"

        self._api_client = ApiClient(host=self.host, token=self.token)
        for service_name, service in DatabricksAPI._get_services():
            setattr(self, service_name, service(self._api_client))

    @staticmethod
    def _get_services():
        for service_name, service in services.__dict__.items():
            if "Service" in service_name:
                yield service_name, service
