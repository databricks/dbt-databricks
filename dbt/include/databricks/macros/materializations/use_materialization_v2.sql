{% macro use_materialization_v2() %}
  {%- set setting = config.get('use_materialization_v2') -%}
  {%- if setting is none -%}
    {{ return(adapter.get_behavior_flag_no_warn('use_materialization_v2')) }}
  {%- endif -%}
  {#-- Quoted YAML values arrive as strings, and 'false' is truthy in Jinja. --#}
  {%- if setting is string -%}
    {{ return(setting | lower == 'true') }}
  {%- endif -%}
  {{ return(setting) }}
{% endmacro %}
