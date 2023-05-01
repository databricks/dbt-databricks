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



