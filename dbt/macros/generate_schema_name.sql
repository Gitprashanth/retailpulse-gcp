-- macros/generate_schema_name.sql
-- Override dbt's default behaviour of prepending the target schema.
-- Without this, dbt builds: retailpulse_silver_retailpulse_silver (doubled).
-- With this, dbt uses the schema name exactly as defined in dbt_project.yml.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
