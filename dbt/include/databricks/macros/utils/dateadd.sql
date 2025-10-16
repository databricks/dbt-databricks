{% macro databricks__dateadd(datepart, interval, from_date_or_timestamp) %}
  {%- if adapter.has_capability(adapter.DBRCapability.TIMESTAMPDIFF) -%}
    timestampadd({{datepart}}, {{interval}}, {{from_date_or_timestamp}})
  {%- else -%}
    {{ spark__dateadd(datepart, interval, from_date_or_timestamp) }}
  {%- endif -%}
{%- endmacro %}
