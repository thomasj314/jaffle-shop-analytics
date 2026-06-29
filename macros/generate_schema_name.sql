{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# prod: 커스텀 스키마 이름 그대로 사용 (gold, silver) #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# dev: target.schema 를 prefix로 붙임 (dev_gold, dev_silver) #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
