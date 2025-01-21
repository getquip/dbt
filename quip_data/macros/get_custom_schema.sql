{#
-- By default, dbt target is always set to dev.
-- If the target name is set to `prod` then tables will be written to the schema specified in `dbt_project.yml` (aka the real schema in the prod dw).
-- Otherwise (for dev) the tables will be written out to a custom schema(`your_name_schema_name`)
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

        {%- if target.name == "dev" and 
            custom_database_name is not none and
            custom_schema_name is not none
        -%}
            {{ default_schema }}_{{ custom_schema_name }}
        {%- else -%}
            {{ custom_schema_name }}
        {%- endif -%}
{%- endmacro %}


{% macro generate_database_name(custom_database_name, node) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{ default_database }}
    
    {%- elif
        target.name != 'prod' and
        custom_database_name is not none
    -%}

        {{ custom_database_name | trim }}-dev

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}
