<p align="center">
  <img src="https://bynder-public-us-west-2.s3.amazonaws.com/styleguide/ABB317701CA31CB7F29268E32B303CAE-pdf-column-1.png" alt="databricks logo" width="50%" />
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>
<p align="center">
  <a href="https://github.com/databricks/dbt-databricks/actions/workflows/main.yml">
    <img src="https://github.com/databricks/dbt-databricks/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
  <a href="https://github.com/databricks/dbt-databricks/actions/workflows/integration.yml">
    <img src="https://github.com/databricks/dbt-databricks/actions/workflows/integration.yml/badge.svg?event=push" alt="Integration Tests Badge"/>
  </a>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

The **[Databricks Lakehouse](https://www.databricks.com/)** provides one simple platform to unify all your data, analytics and AI workloads.

# dbt-databricks

The `dbt-databricks` adapter contains all of the code enabling dbt to work with Databricks. This adapter is based off the amazing work done in [dbt-spark](https://github.com/dbt-labs/dbt-spark). Some key features include:

- **Easy setup**. No need to install an ODBC driver as the adapter uses pure Python APIs.
- **Open by default**. For example, it uses the the open and performant [Delta](https://delta.io/) table format by default. This has many benefits, including letting you use `MERGE` as the the default incremental materialization strategy.
- **Support for Unity Catalog**. dbt-databricks supports the 3-level namespace of Unity Catalog (catalog / schema / relations) so you can organize and secure your data the way you like.
- **Performance**. The adapter generates SQL expressions that are automatically accelerated by the native, vectorized [Photon](https://databricks.com/product/photon) execution engine.

## Choosing between dbt-databricks and dbt-spark

If you are developing a dbt project on Databricks, we recommend using `dbt-databricks` for the reasons noted above.

`dbt-spark` is an actively developed adapter which works with Databricks as well as Apache Spark anywhere it is hosted e.g. on AWS EMR.

## Getting started

### Installation

Install using pip:

```nofmt
pip install dbt-databricks
```

Upgrade to the latest version

```nofmt
pip install --upgrade dbt-databricks
```

### Profile Setup

```nofmt
your_profile_name:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: [optional catalog name, if you are using Unity Catalog]
      schema: [database/schema name]
      host: [your.databrickshost.com]
      http_path: [/sql/your/http/path]
      token: [dapiXXXXXXXXXXXXXXXXXXXXXXX]
```

### Authentication

The adapter supports all [Databricks unified authentication](https://docs.databricks.com/dev-tools/auth/unified-auth.html) methods. For a full setup walkthrough, see the **[Connect to Databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)** guide on docs.getdbt.com.

The method is selected automatically based on which fields are present in your profile. Priority order (first match wins):

| Method | Required profile fields |
|---|---|
| Personal Access Token (PAT) | `token` |
| Azure service principal | `azure_client_id` + `azure_client_secret` |
| Any explicit SDK auth type | `auth_type` (see values below) |
| OAuth user-to-machine (browser) | _(none of the above — opens browser)_ |
| OAuth M2M / legacy Azure SP | `client_secret` (tries both automatically) |

#### `auth_type` values

Set `auth_type` in your profile to delegate entirely to the Databricks SDK for that auth method:

| `auth_type` value | Description |
|---|---|
| `oauth` | U2M browser login (legacy dbt alias for `external-browser`) |
| `oauth-m2m` | Service principal via OAuth M2M; requires `client_id` + `client_secret` |
| `azure-cli` | Azure CLI (`az login`) |
| `azure-msi` | Azure Managed Service Identity |
| `databricks-cli` | Databricks CLI credential chain |
| `google-credentials` | Google service account |
| `metadata-service` | Databricks metadata service |

#### Auth-specific profile fields

```nofmt
# OAuth M2M / service principal
client_id: ...
client_secret: ...

# Azure service principal (explicit)
azure_client_id: ...
azure_client_secret: ...

# Azure common options
azure_tenant_id: ...
azure_environment: ...          # e.g. usgovernment
azure_workspace_resource_id: ...

# Azure MSI (user-assigned identity)
auth_type: azure-msi
azure_client_id: ...            # omit for system-assigned

# Databricks CLI
auth_type: databricks-cli
databricks_cli_profile: ...     # optional: named profile in ~/.databrickscfg

# Google
auth_type: google-credentials
google_credentials: ...         # path to service account JSON
google_service_account: ...

# Metadata service / OIDC
metadata_service_url: ...
oidc_token_env: ...
oidc_token_filepath: ...

# Escape hatch: any extra Databricks SDK Config kwarg not listed above
databricks_sdk_parameters:
  some_sdk_field: value
```

New auth methods added to the Databricks Python SDK are available automatically via `auth_type` + `databricks_sdk_parameters` without requiring an adapter update.

### Documentation

For comprehensive documentation on Databricks-specific features, configurations, and capabilities:

- **[Databricks configurations](https://docs.getdbt.com/reference/resource-configs/databricks-configs)** - Complete reference for all Databricks-specific model configurations, materializations, and incremental strategies
- **[Connect to Databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)** - Setup and authentication guide

### Quick Starts

These following quick starts will get you up and running with the `dbt-databricks` adapter:

- [Set up your dbt project with Databricks](https://docs.getdbt.com/guides/set-up-your-databricks-dbt-project)
- Using dbt Cloud with Databricks ([Azure](https://docs.microsoft.com/en-us/azure/databricks/integrations/prep/dbt-cloud) | [AWS](https://docs.databricks.com/integrations/prep/dbt-cloud.html))
- [Running dbt production jobs on Databricks Workflows](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-workflows.md)
- [Using Unity Catalog with dbt-databricks](https://github.com/databricks/dbt-databricks/blob/main/docs/uc.md)
- [Continuous integration in dbt](https://docs.getdbt.com/docs/deploy/continuous-integration)
- [Loading data from S3 into Delta using the databricks_copy_into macro](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-copy-into-macro-aws.md)
- [Contribute to this repository](CONTRIBUTING.MD)

### Compatibility

The `dbt-databricks` adapter has been tested:

- with Python 3.7 or above.
- against `Databricks SQL` and `Databricks runtime releases 9.1 LTS` and later.

### Tips and Tricks

## Choosing compute for a Python model

You can override the compute used for a specific Python model by setting the `http_path` property in model configuration. This can be useful if, for example, you want to run a Python model on an All Purpose cluster, while running SQL models on a SQL Warehouse. Note that this capability is only available for Python models.

```
def model(dbt, session):
    dbt.config(
      http_path="sql/protocolv1/..."
    )
```

## Python models and ANSI mode

When ANSI mode is enabled (`spark.sql.ansi.enabled=true`), there are limitations when using pandas DataFrames in Python models:

1. **Regular pandas DataFrames**: dbt-databricks will automatically handle conversion even when ANSI mode is enabled, falling back to `spark.createDataFrame()` if needed.

2. **pandas-on-Spark DataFrames**: If you create pandas-on-Spark DataFrames directly in your model (using `pyspark.pandas` or `databricks.koalas`), you may encounter errors with ANSI mode enabled. In this case, you have two options:
   - Disable ANSI mode for your session: Set `spark.sql.ansi.enabled=false` in your cluster or SQL warehouse configuration
   - Set the pandas-on-Spark option in your model code:
     ```python
     import pyspark.pandas as ps
     ps.set_option('compute.fail_on_ansi_mode', False)
     ```
     Note: This may cause unexpected behavior as pandas-on-Spark follows pandas semantics (returning null/NaN for invalid operations) rather than ANSI SQL semantics (raising errors).

For more information about ANSI mode and its implications, see the [Spark documentation on ANSI compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html).
