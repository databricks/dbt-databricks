# Configure OAuth for DBT Databricks

This feature is in [Public Preview](https://docs.databricks.com/release-notes/release-types.html).

Databricks DBT adapter now supports authentication via OAuth in AWS and Azure. This is a much safer method as it enables you to generate short-lived (one hour) OAuth access tokens, which eliminates the risk of accidentally exposing longer-lived tokens such as Databricks personal access tokens through version control checkins or other means. OAuth also enables better server-side session invalidation and scoping.

Once an admin correctly configured OAuth in Databricks, you can simply add the config `auth_type` and set it to `oauth`. Config `token` is no longer necessary. 

For Azure, you admin needs to create a Public AD application for dbt and provide you with its client_id.

``` YAML
jaffle_shop:
  outputs:
    dev:
      host: <databricks host name>
      http_path: <http path for warehouse or cluster>
      catalog: <UC catalog name>
      schema: <schema name>
      auth_type: oauth  # new
      client_id: <azure application ID> # only necessary for Azure
      type: databricks
  target: dev
```

## Troubleshooting

DBT expects the OAuth application to have the "All APIs" scope and redirect URL `http://localhost:8020` by default.

If the OAuth application has only been configured with SQL access scopes or a custom redirect URL, you may need to update your profile accordingly:

``` YAML
jaffle_shop:
  outputs:
    dev:
      host: <databricks host name>
      http_path: <http path for warehouse or cluster>
      catalog: <UC catalog name>
      schema: <schema name>
      auth_type: oauth  # new
      client_id: <azure application ID> # only necessary for Azure
      oauth_redirect_url: https://example.com
      oauth_scopes:
        - sql
        - offline_access
      type: databricks
  target: dev
```

You can find these settings in [Account Console](https://accounts.cloud.databricks.com) > [Settings](https://accounts.cloud.databricks.com/settings) > [App Connections](https://accounts.cloud.databricks.com/settings/app-integrations) > dbt adapter for Databricks



If you encounter any issues, please refer to the [OAuth user-to-machine (U2M) authentication guide](https://docs.databricks.com/en/dev-tools/auth/oauth-u2m.html).

