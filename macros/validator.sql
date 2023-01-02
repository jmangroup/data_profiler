{% macro variable_validator(target_database, target_schema, target_table, source_database, source_schema, exclude_tables, include_tables) %}
    
    -- source database validation
    {% if source_database == '' %}
        {{ exceptions.raise_compiler_error(" `source_database` should should not be empty ") }}
    {% elif '[' in source_database | string %}
        {{ exceptions.raise_compiler_error(" `source_database` should not be a list ") }}
    {% endif %}

    -- source schema validation
    {% if '[' not in source_schema | string %}
        {{ exceptions.raise_compiler_error(" `source_schema` should not be a string ") }}
    {% endif %}

    -- target database validation
    {% if target_database == '' %}
        {{ exceptions.raise_compiler_error(" `target_database` should not be empty ") }}
    {% elif '[' in target_database | string %}
        {{ exceptions.raise_compiler_error(" `target_database` should not be a list ") }}
    {% endif %}

    -- target schema validation
    {% if target_schema == '' %}
        {{ exceptions.raise_compiler_error(" `target_schema` should not be empty ") }}
    {% elif '[' in target_schema | string %}
        {{ exceptions.raise_compiler_error(" `target_schema` should not be a list ") }}
    {% endif %}

    -- target table validation
    {% if target_table == '' %}
        {{ exceptions.raise_compiler_error(" `target_table` should not be empty ") }}
    {% elif '[' in target_table | string %}
        {{ exceptions.raise_compiler_error(" `target_table` should not be a list ") }}
    {% endif %}

    -- exclude table validation
    {% if '[' not in exclude_tables | string %}
        {{ exceptions.raise_compiler_error(" `exclude_tables` should not be a string ") }}
    {% endif %}

    -- include table validation
    {% if '[' not in include_tables | string %}
        {{ exceptions.raise_compiler_error(" `include_tables` should not be a string ") }}
    {% endif %}

{%- endmacro %}