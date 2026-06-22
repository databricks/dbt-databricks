{#
  dbt-core only registers `doc()` in the schema-YAML render context, so it is
  undefined when rendering a model body. Metric view models carry their docs in
  the model body (YAML `comment:` / `display_name:` fields), so `{{ doc(...) }}`
  there fails with "'doc' is undefined". Defining `doc` as a macro fills that gap:
  the model render context has no `doc` member, so Jinja falls back to this macro,
  while the schema-YAML context keeps using dbt-core's built-in `doc()` (it carries
  no macro namespace), leaving description rendering untouched.

  Accepts `doc('name')` or `doc('package', 'name')`, matching dbt-core's `doc()`.
#}
{% macro doc() %}
  {#- During parse the manifest's doc lookup is not built yet; defer resolution
      to execution, mirroring dbt-core where parse-time doc() is a no-op. -#}
  {%- if not execute -%}
    {{- return('') -}}
  {%- endif -%}
  {%- if varargs | length == 1 -%}
    {%- set doc_package = none -%}
    {%- set doc_name = varargs[0] -%}
  {%- elif varargs | length == 2 -%}
    {%- set doc_package = varargs[0] -%}
    {%- set doc_name = varargs[1] -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error("doc() takes exactly one or two arguments (" ~ (varargs | length) ~ " given)") }}
  {%- endif -%}
  {%- set node_package = model.package_name if model is defined and model else none -%}
  {%- set block_contents = adapter.resolve_doc(doc_name, doc_package, node_package) -%}
  {%- if block_contents is none -%}
    {{ exceptions.raise_compiler_error("Documentation for '" ~ doc_name ~ "' not found") }}
  {%- endif -%}
  {{- return(block_contents) -}}
{% endmacro %}
