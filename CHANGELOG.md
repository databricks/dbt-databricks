## dbt-databricks 1.6.0 (Release TBD)

### Features
- Added support for materialized_view and streaming_table materializations

### Fixes

- Fix issue where the show tables extended command is limited to 2048 characters. ([#326](https://github.com/databricks/dbt-databricks/pull/326))
- Extend python model support to cover the same config options as SQL ([#379](https://github.com/databricks/dbt-databricks/pull/379))

## dbt-databricks 1.5.5 (July 7, 2023)

### Fixes

- Fixed issue where starting a terminated cluster in the python path would never return

### Features

- Include log events from databricks-sql-connector in dbt logging output.
- Adapter now populates the `query_id` field in `run_results.json` with Query History API query ID.

## dbt-databricks 1.5.4 (June 9, 2023)

### Features
- Added support for model contracts ([#336](https://github.com/databricks/dbt-databricks/pull/336))

## dbt-databricks 1.5.3 (June 8, 2023)

### Fixes
- Pins dependencies to minor versions
- Sets default socket timeout to 180s

## dbt-databricks 1.5.2 (May 17, 2023)

### Fixes
- Sets databricks sdk dependency to 0.1.6 to avoid SDK breaking changes

## dbt-databricks 1.5.1 (May 9, 2023)

### Fixes
- Add explicit dependency to protobuf >4 to work around dbt-core issue

## dbt-databricks 1.5.0 (May 2, 2023)

### Features
- Added support for OAuth (SSO and client credentials) ([#327](https://github.com/databricks/dbt-databricks/pull/327))
### Fixes
- Fix integration tests ([#316](https://github.com/databricks/dbt-databricks/pull/316))

### Dependencies
- Updated dbt-spark from >=1.4.1 to >= 1.5.0 ([#316](https://github.com/databricks/dbt-databricks/pull/316))

### Under the hood
- Throw an error if a model has an enforced contract. ([#322](https://github.com/databricks/dbt-databricks/pull/322))

## dbt-databricks 1.4.3 (April 19, 2023)

### Fixes
- fix database not found error matching ([#281](https://github.com/databricks/dbt-databricks/pull/281))
- Auto start cluster for Python models ([#306](https://github.com/databricks/dbt-databricks/pull/306))
- databricks-sql-connector to 2.5.0 ([#311](https://github.com/databricks/dbt-databricks/pull/311))

### Features
- Adding replace_where incremental strategy ([#293](https://github.com/databricks/dbt-databricks/pull/293)) ([#310](https://github.com/databricks/dbt-databricks/pull/310))
- [feat] Support ZORDER as a model config ([#292](https://github.com/databricks/dbt-databricks/pull/293)) ([#297](https://github.com/databricks/dbt-databricks/pull/297))

### Dependencies

- Added keyring>=23.13.0 for oauth token cache
- Added databricks-sdk>=0.1.1 for oauth flows
- Updated databricks-sql-connector from >=2.4.0 to >= 2.5.0

### Under the hood
Throw an error if a model has an enforced contract. ([#322](https://github.com/databricks/dbt-databricks/pull/322))

## dbt-databricks 1.4.2 (February 17, 2023)

### Fixes
- Fix test_grants to use the error class to check the error. ([#273](https://github.com/databricks/dbt-databricks/pull/273))
- Raise exception on unexpected error of list relations ([#270](https://github.com/databricks/dbt-databricks/pull/270))

## dbt-databricks 1.4.1 (January 31, 2023)

### Fixes
- Ignore case sensitivity in relation matches method. ([#265](https://github.com/databricks/dbt-databricks/pull/265))

## dbt-databricks 1.4.0 (January 25, 2023)

### Breaking changes
- Raise an exception when schema contains '.'. ([#222](https://github.com/databricks/dbt-databricks/pull/222))
    - Containing a catalog in `schema` is not allowed anymore.
    - Need to explicitly use `catalog` instead.

### Features
- Support Python 3.11 ([#233](https://github.com/databricks/dbt-databricks/pull/233))
- Support `incremental_predicates` ([#161](https://github.com/databricks/dbt-databricks/pull/161))
- Apply connection retry refactor, add defaults with exponential backoff ([#137](https://github.com/databricks/dbt-databricks/pull/137))
- Quote by Default ([#241](https://github.com/databricks/dbt-databricks/pull/241))
- Avoid show table extended command. ([#231](https://github.com/databricks/dbt-databricks/pull/231))
- Use show table extended with table name list for get_catalog. ([#237](https://github.com/databricks/dbt-databricks/pull/237))
- Add support for a glob pattern in the databricks_copy_into macro ([#259](https://github.com/databricks/dbt-databricks/pull/259))

## dbt-databricks 1.3.2 (November 9, 2022)

### Fixes
- Fix copy into macro when passing `expression_list`. ([#223](https://github.com/databricks/dbt-databricks/pull/223))
- Partially revert to fix the case where schema config contains uppercase letters. ([#224](https://github.com/databricks/dbt-databricks/pull/224))

## dbt-databricks 1.3.1 (November 1, 2022)

### Under the hood
- Show and log a warning when schema contains '.'. ([#221](https://github.com/databricks/dbt-databricks/pull/221))

## dbt-databricks 1.3.0 (October 14, 2022)

### Features
- Support python model through run command API, currently supported materializations are table and incremental. ([dbt-labs/dbt-spark#377](https://github.com/dbt-labs/dbt-spark/pull/377), [#126](https://github.com/databricks/dbt-databricks/pull/126))
- Enable Pandas and Pandas-on-Spark DataFrames for dbt python models ([dbt-labs/dbt-spark#469](https://github.com/dbt-labs/dbt-spark/pull/469), [#181](https://github.com/databricks/dbt-databricks/pull/181))
- Support job cluster in notebook submission method ([dbt-labs/dbt-spark#467](https://github.com/dbt-labs/dbt-spark/pull/467), [#194](https://github.com/databricks/dbt-databricks/pull/194))
    - In `all_purpose_cluster` submission method, a config `http_path` can be specified in Python model config to switch the cluster where Python model runs.
      ```py
      def model(dbt, _):
          dbt.config(
              materialized='table',
              http_path='...'
          )
          ...
      ```
- Use builtin timestampadd and timestampdiff functions for dateadd/datediff macros if available ([#185](https://github.com/databricks/dbt-databricks/pull/185))
- Implement testing for a test for various Python models ([#189](https://github.com/databricks/dbt-databricks/pull/189))
- Implement testing for `type_boolean` in Databricks ([dbt-labs/dbt-spark#471](https://github.com/dbt-labs/dbt-spark/pull/471), [#188](https://github.com/databricks/dbt-databricks/pull/188))
- Add a macro to support [COPY INTO](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html) ([#190](https://github.com/databricks/dbt-databricks/pull/190))

### Under the hood
- Apply "Initial refactoring of incremental materialization" ([#148](https://github.com/databricks/dbt-databricks/pull/148))
    - Now dbt-databricks uses `adapter.get_incremental_strategy_macro` instead of `dbt_spark_get_incremental_sql` macro to dispatch the incremental strategy macro. The overwritten `dbt_spark_get_incremental_sql` macro will not work anymore.
- Better interface for python submission ([dbt-labs/dbt-spark#452](https://github.com/dbt-labs/dbt-spark/pull/452), [#178](https://github.com/databricks/dbt-databricks/pull/178))

## dbt-databricks 1.2.3 (September 26, 2022)

### Fixes
- Fix cancellation ([#173](https://github.com/databricks/dbt-databricks/pull/173))
- `http_headers` should be dict in the profile ([#174](https://github.com/databricks/dbt-databricks/pull/174))

## dbt-databricks 1.2.2 (September 8, 2022)

### Fixes
- Data is duplicated on reloading seeds that are using an external table ([#114](https://github.com/databricks/dbt-databricks/issues/114), [#149](https://github.com/databricks/dbt-databricks/issues/149))

### Under the hood
- Explicitly close cursors ([#163](https://github.com/databricks/dbt-databricks/pull/163))
- Upgrade databricks-sql-connector to 2.0.5 ([#166](https://github.com/databricks/dbt-databricks/pull/166))
- Embed dbt-databricks and databricks-sql-connector versions to SQL comments ([#167](https://github.com/databricks/dbt-databricks/pull/167))

## dbt-databricks 1.2.1 (August 24, 2022)

### Features
- Support Python 3.10 ([#158](https://github.com/databricks/dbt-databricks/pull/158))

## dbt-databricks 1.2.0 (August 16, 2022)

### Features
- Add grants to materializations ([dbt-labs/dbt-spark#366](https://github.com/dbt-labs/dbt-spark/issues/366), [dbt-labs/dbt-spark#381](https://github.com/dbt-labs/dbt-spark/pull/381))
- Add `connection_parameters` for databricks-sql-connector connection parameters ([#135](https://github.com/databricks/dbt-databricks/pull/135))
    - This can be used to customize the connection by setting additional parameters.
    - The full parameters are listed at [Databricks SQL Connector for Python](https://docs.databricks.com/dev-tools/python-sql-connector.html#connect-method).
    - Currently, the following parameters are reserved for `dbt-databricks`. Please use the normal credential settings instead.
        - server_hostname
        - http_path
        - access_token
        - session_configuration
        - catalog
        - schema

### Fixes
- Incremental materialization updated to not drop table first if full refresh for delta lake format, as it already runs _create or replace table_ ([dbt-labs/dbt-spark#286](https://github.com/dbt-labs/dbt-spark/issues/286), [dbt-labs/dbt-spark#287](https://github.com/dbt-labs/dbt-spark/pull/287))

### Under the hood
- Update `SparkColumn.numeric_type` to return `decimal` instead of `numeric`, since SparkSQL exclusively supports the former ([dbt-labs/dbt-spark#380](https://github.com/dbt-labs/dbt-spark/pull/380))
- Make minimal changes to support dbt Core incremental materialization refactor ([dbt-labs/dbt-spark#402](https://github.com/dbt-labs/dbt-spark/issue/402), [dbt-labs/dbt-spark#394](httpe://github.com/dbt-labs/dbt-spark/pull/394), [#136](https://github.com/databricks/dbt-databricks/pull/136))
- Add new basic tests `TestDocsGenerateDatabricks` and `TestDocsGenReferencesDatabricks` ([#134](https://github.com/databricks/dbt-databricks/pull/134))
- Set upper bound for `databricks-sql-connector` when Python 3.10 ([#154](https://github.com/databricks/dbt-databricks/pull/154))
    - Note that `databricks-sql-connector` does not officially support Python 3.10 yet.

### Contributors
- [@grindheim](https://github.com/grindheim) ([dbt-labs/dbt-spark#287](https://github.com/dbt-labs/dbt-spark/pull/287/))

## dbt-databricks 1.1.1 (July 19, 2022)

### Features
- Support for Databricks CATALOG as a DATABASE in DBT compilations ([#95](https://github.com/databricks/dbt-databricks/issues/95), [#89](https://github.com/databricks/dbt-databricks/pull/89), [#94](https://github.com/databricks/dbt-databricks/pull/94), [#105](https://github.com/databricks/dbt-databricks/pull/105))
    - Setting an initial catalog with `session_properties` is deprecated and will not work in the future release. Please use `catalog` or `database` to set the initial catalog.
    - When using catalog, `spark_build_snapshot_staging_table` macro will not be used. If trying to override the macro, `databricks_build_snapshot_staging_table` should be overridden instead.

### Fixes
- Block taking jinja2.runtime.Undefined into DatabricksAdapter ([#98](https://github.com/databricks/dbt-databricks/pull/98))
- Avoid using Cursor.schema API when database is None ([#100](https://github.com/databricks/dbt-databricks/pull/100))

### Under the hood
- Drop databricks-sql-connector 1.0 ([#108](https://github.com/databricks/dbt-databricks/pull/108))

## dbt-databricks 1.1.0 (May 11, 2022)

### Features
- Add support for [Delta constraints](https://docs.databricks.com/delta/delta-constraints.html) ([#71](https://github.com/databricks/dbt-databricks/pull/71))

### Under the hood
- Port testing framework changes from [dbt-labs/dbt-spark#299](https://github.com/dbt-labs/dbt-spark/pull/299) and [dbt-labs/dbt-spark#314](https://github.com/dbt-labs/dbt-spark/pull/314) ([#70](https://github.com/databricks/dbt-databricks/pull/70))

## dbt-databricks 1.0.3 (April 26, 2022)

### Fixes
- Make internal macros use macro dispatch pattern ([#72](https://github.com/databricks/dbt-databricks/pull/72))

## dbt-databricks 1.0.2 (March 31, 2022)

### Features
- Support for setting table properties as part of a model configuration ([#33](https://github.com/databricks/dbt-databricks/issues/33), [#49](https://github.com/databricks/dbt-databricks/pull/49))
- Get the session_properties map to work ([#57](https://github.com/databricks/dbt-databricks/pull/57))
- Bump up databricks-sql-connector to 1.0.1 and use the Cursor APIs ([#50](https://github.com/databricks/dbt-databricks/pull/50))

## dbt-databricks 1.0.1 (February 8, 2022)

### Features
- Inherit from dbt-spark for backward compatibility with spark-utils and other dbt packages ([#32](https://github.com/databricks/dbt-databricks/issues/32), [#35](https://github.com/databricks/dbt-databricks/pull/35))
- Add SQL Endpoint specific integration tests ([#45](https://github.com/databricks/dbt-databricks/pull/45), [#46](https://github.com/databricks/dbt-databricks/pull/46))

### Fixes
- Close the connection properly ([#34](https://github.com/databricks/dbt-databricks/issues/34), [#37](https://github.com/databricks/dbt-databricks/pull/37))

## dbt-databricks 1.0.0 (December 6, 2021)

### Features
- Make the connection use databricks-sql-connector ([#3](https://github.com/databricks/dbt-databricks/pull/3), [#7](https://github.com/databricks/dbt-databricks/pull/7))
- Make the default file format 'delta' ([#14](https://github.com/databricks/dbt-databricks/pull/14), [#16](https://github.com/databricks/dbt-databricks/pull/16))
- Make the default incremental strategy 'merge' ([#23](https://github.com/databricks/dbt-databricks/pull/23))
- Remove unnecessary stack trace ([#10](https://github.com/databricks/dbt-databricks/pull/10))

## dbt-spark 1.0.0 (December 3, 2021)

### Fixes
- Incremental materialization corrected to respect `full_refresh` config, by using `should_full_refresh()` macro ([#260](https://github.com/dbt-labs/dbt-spark/issues/260), [#262](https://github.com/dbt-labs/dbt-spark/pull/262/))

### Contributors
- [@grindheim](https://github.com/grindheim) ([#262](https://github.com/dbt-labs/dbt-spark/pull/262/))

## dbt-spark 1.0.0rc2 (November 24, 2021)

### Features
- Add support for Apache Hudi (hudi file format) which supports incremental merge strategies ([#187](https://github.com/dbt-labs/dbt-spark/issues/187), [#210](https://github.com/dbt-labs/dbt-spark/pull/210))

### Under the hood
- Refactor seed macros: remove duplicated code from dbt-core, and provide clearer logging of SQL parameters that differ by connection method ([#249](https://github.com/dbt-labs/dbt-spark/issues/249), [#250](https://github.com/dbt-labs/dbt-snowflake/pull/250))
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#247](https://github.com/dbt-labs/dbt-spark/pull/247))

### Contributors
- [@vingov](https://github.com/vingov) ([#210](https://github.com/dbt-labs/dbt-spark/pull/210))

## dbt-spark 1.0.0rc1 (November 10, 2021)

### Under the hood
- Remove official support for python 3.6, which is reaching end of life on December 23, 2021 ([dbt-core#4134](https://github.com/dbt-labs/dbt-core/issues/4134), [#253](https://github.com/dbt-labs/dbt-snowflake/pull/253))
- Add support for structured logging ([#251](https://github.com/dbt-labs/dbt-spark/pull/251))

## dbt-spark 0.21.1 (Release TBD)

## dbt-spark 0.21.1rc1 (November 3, 2021)

### Fixes
- Fix `--store-failures` for tests, by suppressing irrelevant error in `comment_clause()` macro ([#232](https://github.com/dbt-labs/dbt-spark/issues/232), [#233](https://github.com/dbt-labs/dbt-spark/pull/233))
- Add support for `on_schema_change` config in incremental models: `ignore`, `fail`, `append_new_columns`. For `sync_all_columns`, removing columns is not supported by Apache Spark or Delta Lake ([#198](https://github.com/dbt-labs/dbt-spark/issues/198), [#226](https://github.com/dbt-labs/dbt-spark/issues/226), [#229](https://github.com/dbt-labs/dbt-spark/pull/229))
- Add `persist_docs` call to incremental model ([#224](https://github.com/dbt-labs/dbt-spark/issues/224), [#234](https://github.com/dbt-labs/dbt-spark/pull/234))

### Contributors
- [@binhnefits](https://github.com/binhnefits) ([#234](https://github.com/dbt-labs/dbt-spark/pull/234))

## dbt-spark 0.21.0 (October 4, 2021)

### Fixes
- Enhanced get_columns_in_relation method to handle a bug in open source deltalake which doesnt return schema details in `show table extended in databasename like '*'` query output. This impacts dbt snapshots if file format is open source deltalake ([#207](https://github.com/dbt-labs/dbt-spark/pull/207))
- Parse properly columns when there are struct fields to avoid considering inner fields: Issue ([#202](https://github.com/dbt-labs/dbt-spark/issues/202))

### Under the hood
- Add `unique_field` to better understand adapter adoption in anonymous usage tracking ([#211](https://github.com/dbt-labs/dbt-spark/pull/211))

### Contributors
- [@harryharanb](https://github.com/harryharanb) ([#207](https://github.com/dbt-labs/dbt-spark/pull/207))
- [@SCouto](https://github.com/Scouto) ([#204](https://github.com/dbt-labs/dbt-spark/pull/204))

## dbt-spark 0.21.0b2 (August 20, 2021)

### Fixes
- Add pyodbc import error message to dbt.exceptions.RuntimeException to get more detailed information when running `dbt debug` ([#192](https://github.com/dbt-labs/dbt-spark/pull/192))
- Add support for ODBC Server Side Parameters, allowing options that need to be set with the `SET` statement to be used ([#201](https://github.com/dbt-labs/dbt-spark/pull/201))
- Add `retry_all` configuration setting to retry all connection issues, not just when the `_is_retryable_error` function determines ([#194](https://github.com/dbt-labs/dbt-spark/pull/194))

### Contributors
- [@JCZuurmond](https://github.com/JCZuurmond) ([#192](https://github.com/fishtown-analytics/dbt-spark/pull/192))
- [@jethron](https://github.com/jethron) ([#201](https://github.com/fishtown-analytics/dbt-spark/pull/201))
- [@gregingenii](https://github.com/gregingenii) ([#194](https://github.com/dbt-labs/dbt-spark/pull/194))

## dbt-spark 0.21.0b1 (August 3, 2021)

## dbt-spark 0.20.1 (August 2, 2021)

## dbt-spark 0.20.1rc1 (August 2, 2021)

### Fixes
- Fix `get_columns_in_relation` when called on models created in the same run ([#196](https://github.com/dbt-labs/dbt-spark/pull/196), [#197](https://github.com/dbt-labs/dbt-spark/pull/197))

### Contributors
- [@ali-tny](https://github.com/ali-tny) ([#197](https://github.com/fishtown-analytics/dbt-spark/pull/197))


## dbt-spark 0.20.0 (July 12, 2021)

## dbt-spark 0.20.0rc2 (July 7, 2021)

### Features

- Add support for `merge_update_columns` config in `merge`-strategy incremental models ([#183](https://github.com/fishtown-analytics/dbt-spark/pull/183), [#184](https://github.com/fishtown-analytics/dbt-spark/pull/184))

### Fixes

- Fix column-level `persist_docs` on Delta tables, add tests ([#180](https://github.com/fishtown-analytics/dbt-spark/pull/180))

## dbt-spark 0.20.0rc1 (June 8, 2021)

### Features

- Allow user to specify `use_ssl` ([#169](https://github.com/fishtown-analytics/dbt-spark/pull/169))
- Allow setting table `OPTIONS` using `config` ([#171](https://github.com/fishtown-analytics/dbt-spark/pull/171))
- Add support for column-level `persist_docs` on Delta tables ([#84](https://github.com/fishtown-analytics/dbt-spark/pull/84), [#170](https://github.com/fishtown-analytics/dbt-spark/pull/170))

### Fixes
- Cast `table_owner` to string to avoid errors generating docs ([#158](https://github.com/fishtown-analytics/dbt-spark/pull/158), [#159](https://github.com/fishtown-analytics/dbt-spark/pull/159))
- Explicitly cast column types when inserting seeds ([#139](https://github.com/fishtown-analytics/dbt-spark/pull/139), [#166](https://github.com/fishtown-analytics/dbt-spark/pull/166))

### Under the hood
- Parse information returned by `list_relations_without_caching` macro to speed up catalog generation ([#93](https://github.com/fishtown-analytics/dbt-spark/issues/93), [#160](https://github.com/fishtown-analytics/dbt-spark/pull/160))
- More flexible host passing, https:// can be omitted ([#153](https://github.com/fishtown-analytics/dbt-spark/issues/153))

### Contributors
- [@friendofasquid](https://github.com/friendofasquid) ([#159](https://github.com/fishtown-analytics/dbt-spark/pull/159))
- [@franloza](https://github.com/franloza) ([#160](https://github.com/fishtown-analytics/dbt-spark/pull/160))
- [@Fokko](https://github.com/Fokko) ([#165](https://github.com/fishtown-analytics/dbt-spark/pull/165))
- [@rahulgoyal2987](https://github.com/rahulgoyal2987) ([#169](https://github.com/fishtown-analytics/dbt-spark/pull/169))
- [@JCZuurmond](https://github.com/JCZuurmond) ([#171](https://github.com/fishtown-analytics/dbt-spark/pull/171))
- [@cristianoperez](https://github.com/cristianoperez) ([#170](https://github.com/fishtown-analytics/dbt-spark/pull/170))


## dbt-spark 0.19.1 (April 2, 2021)

## dbt-spark 0.19.1b2 (February 26, 2021)

### Under the hood
- Update serialization calls to use new API in dbt-core `0.19.1b2` ([#150](https://github.com/fishtown-analytics/dbt-spark/pull/150))

## dbt-spark 0.19.0.1 (February 26, 2021)

### Fixes
- Fix package distribution to include incremental model materializations ([#151](https://github.com/fishtown-analytics/dbt-spark/pull/151), [#152](https://github.com/fishtown-analytics/dbt-spark/issues/152))

## dbt-spark 0.19.0 (February 21, 2021)

### Breaking changes
- Incremental models have `incremental_strategy: append` by default. This strategy adds new records without updating or overwriting existing records. For that, use `merge` or `insert_overwrite` instead, depending on the file format, connection method, and attributes of your underlying data. dbt will try to raise a helpful error if you configure a strategy that is not supported for a given file format or connection. ([#140](https://github.com/fishtown-analytics/dbt-spark/pull/140), [#141](https://github.com/fishtown-analytics/dbt-spark/pull/141))

### Fixes
- Capture hard-deleted records in snapshot merge, when `invalidate_hard_deletes` config is set ([#109](https://github.com/fishtown-analytics/dbt-spark/pull/143), [#126](https://github.com/fishtown-analytics/dbt-spark/pull/144))

## dbt-spark 0.19.0rc1 (January 8, 2021)

### Breaking changes
- Users of the `http` and `thrift` connection methods need to install extra requirements: `pip install dbt-spark[PyHive]` ([#109](https://github.com/fishtown-analytics/dbt-spark/pull/109), [#126](https://github.com/fishtown-analytics/dbt-spark/pull/126))

### Under the hood
- Enable `CREATE OR REPLACE` support when using Delta. Instead of dropping and recreating the table, it will keep the existing table, and add a new version as supported by Delta. This will ensure that the table stays available when running the pipeline, and you can track the history.
- Add changelog, issue templates ([#119](https://github.com/fishtown-analytics/dbt-spark/pull/119), [#120](https://github.com/fishtown-analytics/dbt-spark/pull/120))

### Fixes
- Handle case of 0 retries better for HTTP Spark Connections ([#132](https://github.com/fishtown-analytics/dbt-spark/pull/132))

### Contributors
- [@danielvdende](https://github.com/danielvdende) ([#132](https://github.com/fishtown-analytics/dbt-spark/pull/132))
- [@Fokko](https://github.com/Fokko) ([#125](https://github.com/fishtown-analytics/dbt-spark/pull/125))

## dbt-spark 0.18.1.1 (November 13, 2020)

### Fixes
- Fix `extras_require` typo to enable `pip install dbt-spark[ODBC]` (([#121](https://github.com/fishtown-analytics/dbt-spark/pull/121)), ([#122](https://github.com/fishtown-analytics/dbt-spark/pull/122)))

## dbt-spark 0.18.1 (November 6, 2020)

### Features
- Allows users to specify `auth` and `kerberos_service_name` ([#107](https://github.com/fishtown-analytics/dbt-spark/pull/107))
- Add support for ODBC driver connections to Databricks clusters and endpoints ([#116](https://github.com/fishtown-analytics/dbt-spark/pull/116))

### Under the hood
- Updated README links ([#115](https://github.com/fishtown-analytics/dbt-spark/pull/115))
- Support complete atomic overwrite of non-partitioned incremental models ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))
- Update to support dbt-core 0.18.1 ([#110](https://github.com/fishtown-analytics/dbt-spark/pull/110), [#118](https://github.com/fishtown-analytics/dbt-spark/pull/118))

### Contributors
- [@danielhstahl](https://github.com/danielhstahl) ([#107](https://github.com/fishtown-analytics/dbt-spark/pull/107))
- [@collinprather](https://github.com/collinprather) ([#115](https://github.com/fishtown-analytics/dbt-spark/pull/115))
- [@charlottevdscheun](https://github.com/charlottevdscheun) ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))
- [@Fokko](https://github.com/Fokko) ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))

## dbt-spark 0.18.0 (September 18, 2020)

### Under the hood
- Make a number of changes to support dbt-adapter-tests ([#103](https://github.com/fishtown-analytics/dbt-spark/pull/103))
- Update to support dbt-core 0.18.0. Run CI tests against local Spark, Databricks ([#105](https://github.com/fishtown-analytics/dbt-spark/pull/105))
