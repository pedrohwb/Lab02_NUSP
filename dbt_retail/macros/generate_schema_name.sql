{#
  generate_schema_name macro override.
  When a custom schema name is provided via +schema config, use it directly
  (instead of the default DBT behavior of prefixing with the target schema).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
