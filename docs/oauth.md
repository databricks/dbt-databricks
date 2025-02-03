# Configure OAuth for DBT Databricks

This feature is in [Public Preview](https://docs.databricks.com/release-notes/release-types.html).

## User to Machine(U2M):

Databricks DBT adapter now supports authentication via OAuth U2M flow in all clouds. This is a much safer method as it enables you to generate short-lived (one hour) OAuth access tokens, which eliminates the risk of accidentally exposing longer-lived tokens such as Databricks personal access tokens through version control checkins or other means. OAuth also enables better server-side session invalidation and scoping.

Simply add the config `auth_type` and set it to `oauth`. Config `token` is no longer necessary.

```YAML
jaffle_shop:
  outputs:
    dev:
      host: <databricks host name>
      http_path: <http path for warehouse or cluster>
      catalog: <UC catalog name>
      schema: <schema name>
      auth_type: oauth  # new
      type: databricks
  target: dev
```

### Troubleshooting

DBT expects the OAuth application to have the "All APIs" scope and redirect URL `http://localhost:8020` by default.

The default oauth app for dbt-databricks is auto-enabled in every account with expected settings, you can find it in [Account Console](https://accounts.cloud.databricks.com) > [Settings](https://accounts.cloud.databricks.com/settings) > [App Connections](https://accounts.cloud.databricks.com/settings/app-integrations) > dbt adapter for Databricks. If you cannot find it you may have disabled dbt in your account, please refer to this [guide](https://docs.databricks.com/en/integrations/enable-disable-oauth.html) to re-enable dbt as oauth app.

If you encounter any issues, please refer to the [OAuth user-to-machine (U2M) authentication guide](https://docs.databricks.com/en/dev-tools/auth/oauth-u2m.html).

## Machine to Machine(M2M):

Databricks DBT adapter also supports authenticate via OAuth M2M flow in all clouds.
Simply add the config `auth_type` and set it to `oauth`. Follow this [guide](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a databricks service principal and also an OAuth secret. 
Set `client_id` to your databricks service principal id and `client_secret` to your OAuth secret for the service principal.

```YAML
jaffle_shop:
  outputs:
    dev:
      host: <databricks host name>
      http_path: <http path for warehouse or cluster>
      catalog: <UC catalog name>
      schema: <schema name>
      auth_type: oauth  # new
      client_id: <service principal id>
      client_secret: <oauth secret>
      type: databricks
  target: dev
```

### Azure Service Principal
If you are on Azure Databricks and want to use Azure Service Principal, just set `azure_client_id` to your Azure Client Id and `azure_client_secret` to your Azure Client Secret.

```YAML
jaffle_shop:
  outputs:
    dev:
      host: <databricks host name>
      http_path: <http path for warehouse or cluster>
      catalog: <UC catalog name>
      schema: <schema name>
      auth_type: oauth  # new
      azure_client_id: <azure service principal id>
      azure_client_secret: <azure service principal secret>
      type: databricks
  target: dev
```