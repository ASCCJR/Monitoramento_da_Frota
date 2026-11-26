{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if node.resource_type == 'test' -%}
        {%- set resolved_schema = target.schema -%}
    {%- else -%}
        {%- set resolved_schema = custom_schema_name if custom_schema_name is not none else target.schema -%}
    {%- endif -%}
    {%- set allowed_schemas = [
        env_var('FLEET_SILVER_SCHEMA', 'fleet_silver'),
        env_var('FLEET_GOLD_SCHEMA', 'fleet_gold')
    ] -%}

    {%- if resolved_schema not in allowed_schemas -%}
        {{ exceptions.raise_compiler_error(
            "Schema fora da configuração de saída: " ~ resolved_schema
        ) }}
    {%- endif -%}

    {{ resolved_schema }}
{%- endmacro %}
