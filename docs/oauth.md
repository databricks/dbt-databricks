# Configure OAuth for DBT Databricks

This feature is in [Public Preview](https://docs.databricks.com/release-notes/release-types.html).

Databricks DBT adapter now supports authentication via OAuth in AWS and Azure. This is a much safer method as it enables you to generate short-lived (one hour) OAuth access tokens, which eliminates the risk of accidentally exposing longer-lived tokens such as Databricks personal access tokens through version control checkins or other means. OAuth also enables better server-side session invalidation and scoping.

Once an admin correctly configured OAuth in Databricks, you can simply add the config `auth_type` and set it to `oauth`. Config `token` is no longer necessary. 

For Azure, you admin needs to create a Public AD application for dbt and provide you with its client_id.

### SSO Usage
#### AWS
It just works as long as admin has enabled `dbt-databricks` as an OAuth application:
```
curl -n -X POST https://accounts.cloud.databricks.com/api/2.0/accounts/<Account ID>/oauth2/published-app-integrations -d '{ "app_id" : "dbt-databricks" }'
```
Profile:
```yaml
  type: databricks
  host: "<your databricks host name>"
  http_path: "<http path for warehouse >"
  auth_type: "oauth"
```

#### Azure

A Public client/ Native Azure AD Application must be created with redirect URL `http://localhost:8020` and with permission `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/user_impersonation`. 

You must set `client_id` to the Azure client_id created for dbt and set `auth_type` to `oauth`:

Profile:
```yaml
  type: databricks
  host: "<your databricks host name>"
  http_path: "<http path for warehouse >"
  client_id: "<Azure AD Application ID>"
  auth_type: "oauth"
```

#### GCP
GCP is not supported at the moment.

### CI/CD
For automation and CI/CD use cases, `client_id` and `client_secret` from service principals are now available as config as well. When `client_secret` is present the dbt adapter will try to authenticate via 2-legged OAuth flow. Please set `auth_type` to `oauth`.


Profile:
```yaml
  type: databricks
  host: "<your databricks host name>"
  http_path: "<http path for warehouse >"
  client_id: "<Client ID from environment>"
  client_secret: "<Client Secret from environment>"
  auth_type: "oauth"
```