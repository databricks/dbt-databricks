{% macro get_incremental_strategy(file_format) %}
  {% set raw_strategy = config.get('incremental_strategy') or 'merge' %}
  {% do return(dbt_databricks_validate_get_incremental_strategy(raw_strategy, file_format)) %}
{% endmacro %}

{% macro databricks__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro databricks__get_incremental_replace_where_sql(arg_dict) %}
  {% do return(get_replace_where_sql(arg_dict)) %}
{% endmacro %}

{% macro get_incremental_replace_where_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_replace_where_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro databricks__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
    {{ return(get_insert_overwrite_sql(source, target)) }}
{% endmacro %}

{% macro get_insert_overwrite_sql(source_relation, target_relation) %}
    {%- if (adapter.is_cluster() and adapter.compare_dbr_version(17, 1) >= 0) or (not adapter.is_cluster() and adapter.behavior.use_replace_on_for_insert_overwrite) -%}
        {%- if not adapter.is_cluster() %}
            {{ exceptions.warn("insert_overwrite will perform a dynamic insert overwrite. If you depended on the legacy truncation behavior, consider disabling the behavior flag use_replace_on_for_insert_overwrite.") }}
        {%- endif -%}
        {{ get_insert_replace_on_sql(source_relation, target_relation) }}
    {%- else -%}
        {#-- Use legacy DPO INSERT OVERWRITE for older DBR versions and SQL warehouses with behavior flag disabled --#}
        insert overwrite table {{ target_relation }}
        {{ partition_cols(label="partition") }}
        by name
        select * from {{ source_relation }}
    {%- endif -%}
{% endmacro %}

{% macro add_columns_to_list(target_list, columns) %}
    {#-- Local helper to add columns to the target list, converting string to list if needed --#}
    {%- if columns -%}
        {%- if columns is string -%}
            {%- do target_list.append(columns) -%}
        {%- else -%}
            {%- for col in columns -%}
                {%- do target_list.append(col) -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{% macro get_insert_replace_on_sql(source_relation, target_relation) %}
    {%- set partition_by = config.get('partition_by') -%}
    {%- set liquid_clustered_by = config.get('liquid_clustered_by') -%}
    {%- set replace_columns = [] -%}
    
    {#-- If both partition_by and liquid_clustered_by are defined, it will fail before this point with a SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED error from Databricks --#}
    {%- do add_columns_to_list(replace_columns, partition_by) -%}
    {%- do add_columns_to_list(replace_columns, liquid_clustered_by) -%}
    
    {%- if replace_columns -%}
        {%- set replace_conditions = [] -%}
        {%- for col in replace_columns -%}
            {%- do replace_conditions.append('t.' ~ col ~ ' <=> s.' ~ col) -%}
        {%- endfor -%}
        {%- set replace_conditions_csv = replace_conditions | join(' AND ') -%}
        {%- set source_columns = adapter.get_columns_in_relation(source_relation) | map(attribute="name") | list -%}
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute="name") | list -%}
        {%- set source_cols_lower = source_columns | map('lower') | list -%}
        {%- set select_columns = [] -%}
        {%- for dest_col in dest_columns -%}
            {%- set dest_col_lower = dest_col | lower -%}
            {%- set matched_col = namespace(value=none) -%}
            {%- for src_col in source_columns -%}
                {%- if src_col | lower == dest_col_lower and matched_col.value is none -%}
                    {%- set matched_col.value = src_col -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if matched_col.value is not none -%}
                {%- do select_columns.append(matched_col.value) -%}
            {%- else -%}
                {%- do select_columns.append('NULL as ' ~ dest_col) -%}
            {%- endif -%}
        {%- endfor -%}
        insert into table {{ target_relation }} AS t
        replace on ({{ replace_conditions_csv }})
        (select {{ select_columns | join(', ') }} from {{ source_relation }}) AS s
    {%- else -%}
        {#-- Fallback to regular insert overwrite if no partitioning nor liquid clustering defined --#}
        insert overwrite table {{ target_relation }}
        by name
        select * from {{ source_relation }}
    {%- endif -%}
{% endmacro %}

{% macro get_replace_where_sql(args_dict) -%}
  {%- set predicates = args_dict['incremental_predicates'] -%}
  {%- set target_relation = args_dict['target_relation'] -%}
  {%- set temp_relation = args_dict['temp_relation'] -%}
INSERT INTO {{ target_relation.render() }}
{%- if predicates %}
  {%- if predicates is sequence and predicates is not string %}
 REPLACE WHERE {{ predicates | join(' and ') }}
  {%- else %}
 REPLACE WHERE {{ predicates }}
  {%- endif %}
{%- endif %}
 TABLE {{ temp_relation.render() }}
{%- endmacro %}

{% macro get_insert_into_sql(source_relation, target_relation) %}
    {%- set source_columns = adapter.get_columns_in_relation(source_relation) | map(attribute="name") | list -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute="name") | list -%}
    {{ insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) }}
{% endmacro %}

{% macro insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) %}
    {%- set dest_cols_lower = dest_columns | map('lower') | list -%}
    {%- set source_cols_lower = source_columns | map('lower') | list -%}
    {%- if dest_cols_lower | sort == source_cols_lower | sort -%}
        {#-- All columns match (case-insensitive), use simple BY NAME --#}
        insert into {{ target_relation }} by name
        select * from {{ source_relation }}
    {%- else -%}
        {#-- Columns don't match, select only columns that exist in target --#}
        {#-- Note: Cannot use BY NAME with explicit column list, so use traditional syntax --#}
        {%- set common_columns = [] -%}
        {%- for dest_col in dest_columns -%}
            {%- if dest_col | lower in source_cols_lower -%}
                {%- do common_columns.append(dest_col) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if common_columns | length > 0 -%}
            insert into {{ target_relation }} ({{ common_columns | join(', ') }})
            select {{ common_columns | join(', ') }} from {{ source_relation }}
        {%- else -%}
            {#-- No common columns, this shouldn't happen but handle it gracefully --#}
            insert into {{ target_relation }} by name
            select * from {{ source_relation }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

{% macro databricks__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}

  {%- set target_alias = config.get('target_alias', 'DBT_INTERNAL_DEST') -%}
  {%- set source_alias = config.get('source_alias', 'DBT_INTERNAL_SOURCE') -%}

  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set source_columns = (adapter.get_columns_in_relation(source) | map(attribute='name') | list)-%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set merge_with_schema_evolution = (config.get('merge_with_schema_evolution') | lower == 'true') -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
  {%- set skip_matched_step = (config.get('skip_matched_step') | lower == 'true') -%}
  {%- set skip_not_matched_step = (config.get('skip_not_matched_step') | lower == 'true') -%}

  {%- set matched_condition = config.get('matched_condition') -%}
  {%- set not_matched_condition = config.get('not_matched_condition') -%}

  {%- set not_matched_by_source_action = config.get('not_matched_by_source_action') -%}
  {%- set not_matched_by_source_condition = config.get('not_matched_by_source_condition') -%}

  {%- set not_matched_by_source_action_trimmed = not_matched_by_source_action | lower | trim(' \n\t') %}
  {%- set not_matched_by_source_action_is_set = (
      not_matched_by_source_action_trimmed == 'delete'
      or not_matched_by_source_action_trimmed.startswith('update')
    )
  %}
  
  
  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  {{ source_alias }}.{{ adapter.quote(key) }} <=> {{ target_alias }}.{{ adapter.quote(key) }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              {{ source_alias }}.{{ adapter.quote(unique_key) }} <=> {{ target_alias }}.{{ adapter.quote(unique_key) }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

    merge
        {%- if merge_with_schema_evolution %}
        with schema evolution
        {%- endif %}
    into
        {{ target }} as {{ target_alias }}
    using
        {{ source }} as {{ source_alias }}
    on
        {{ predicates | join('\n    and ') }}
    {%- if not skip_matched_step %}
    when matched
        {%- if matched_condition %}
        and ({{ matched_condition }})
        {%- endif %}
        then update set
            {{ get_merge_update_set(update_columns, on_schema_change, source_columns, source_alias) }}
    {%- endif %}
    {%- if not skip_not_matched_step %}
    when not matched
        {%- if not_matched_condition %}
        and ({{ not_matched_condition }})
        {%- endif %}
        then insert
            {{ get_merge_insert(on_schema_change, source_columns, source_alias) }}
    {%- endif %}
    {%- if not_matched_by_source_action_is_set %}
    when not matched by source
        {%- if not_matched_by_source_condition %}
        and ({{ not_matched_by_source_condition }})
        {%- endif %}
        then {{ not_matched_by_source_action }}
    {%- endif %}
{% endmacro %}

{% macro get_merge_update_set(update_columns, on_schema_change, source_columns, source_alias='DBT_INTERNAL_SOURCE') %}
  {%- if update_columns -%}
    {%- for column_name in update_columns -%}
      {{ adapter.quote(column_name) }} = {{ source_alias }}.{{ adapter.quote(column_name) }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- elif on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    {%- for column in source_columns -%}
      {{ adapter.quote(column) }} = {{ source_alias }}.{{ adapter.quote(column) }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- endif -%}
{% endmacro %}

{% macro get_merge_insert(on_schema_change, source_columns, source_alias='DBT_INTERNAL_SOURCE') %}
  {%- if on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    ({% for column in source_columns %}{{ adapter.quote(column) }}{% if not loop.last %}, {% endif %}{% endfor %}) VALUES (
    {%- for column in source_columns -%}
      {{ source_alias }}.{{ adapter.quote(column) }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %})
  {%- endif -%}
{% endmacro %}

{% macro databricks__get_incremental_microbatch_sql(arg_dict) %}
  {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}
  {%- set event_time = model.config.event_time -%}
  {%- set start_time = config.get("__dbt_internal_microbatch_event_time_start") -%}
  {%- set end_time = config.get("__dbt_internal_microbatch_event_time_end") -%}
  {%- if start_time -%}
    {%- do incremental_predicates.append("cast(" ~ event_time ~ " as TIMESTAMP) >= '" ~ start_time ~ "'") -%}
  {%- endif -%}
  {%- if end_time -%}
    {%- do incremental_predicates.append("cast(" ~ event_time ~ " as TIMESTAMP) < '" ~ end_time ~ "'") -%}
  {%- endif -%}
  {%- do arg_dict.update({'incremental_predicates': incremental_predicates}) -%}
  {{ return(get_replace_where_sql(arg_dict)) }}
{% endmacro %}



{% macro databricks__get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {%- set default_cols = None -%}

  {%- if merge_update_columns and merge_exclude_columns -%}
    {{ exceptions.raise_compiler_error(
        'Model cannot specify merge_update_columns and merge_exclude_columns. Please update model to use only one config'
    )}}
  {%- elif merge_update_columns -%}
    {# Don't quote here - columns will be quoted in get_merge_update_set #}
    {%- set update_columns = merge_update_columns -%}
  {%- elif merge_exclude_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
        {%- do update_columns.append(column.column) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set update_columns = default_cols -%}
  {%- endif -%}

  {{ return(update_columns) }}

{% endmacro %}