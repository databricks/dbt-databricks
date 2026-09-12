{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {% call statement(auto_begin=False) %}
        {{ rendered }}
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro run_pre_hooks() %}
  {{ run_hooks(pre_hooks) }}
{% endmacro %}

{% macro run_post_hooks() %}
  {{ run_hooks(post_hooks) }}
{% endmacro %}
