----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Macro for do data profiling based on the parameters we passed
----------------------------------------------------------------------------------------------------------------------------------------------------------
{% macro data_profiling(target_database, target_schema, target_table, source_database, source_schema=[], exclude_tables=[], include_tables=[]) %}
 
    {{ data_profiler.variable_validator(target_database, target_schema, target_table, source_database, source_schema, exclude_tables, include_tables) }}
    
    {% if (flags.WHICH).upper() == 'RUN' or (flags.WHICH).upper() == 'RUN-OPERATION' %}

        {% set profiled_at = data_profiler.create_query(target_database, target_schema, target_table) %}

        -- Read the table names from information schema for that particular layer
        {% set read_information_schema_datas %}
        
            SELECT

                table_catalog           AS table_database
                , table_schema
                , table_name

            FROM {{source_database}}.INFORMATION_SCHEMA.TABLES
            WHERE
                {% if source_schema | length == 0 %}
                    LOWER(table_schema) NOT IN ('information_schema', 'pg_catalog')
                    AND LOWER(table_name) != LOWER('{{ target_table }}')
                {% else %}
                    LOWER(table_schema) IN ( 
                        {%- for profiling_schema in source_schema -%} '{{ profiling_schema.lower() }}' {%- if not loop.last -%} , {% endif -%} {%- endfor -%} )
                {% endif %}

                {% if exclude_tables | length != 0 %}
                    AND LOWER(table_name) NOT IN ( 
                        {%- for exclude_table in exclude_tables -%} '{{ exclude_table.lower() }}' {%- if not loop.last -%} , {% endif -%} {%- endfor -%} )
                {% elif include_tables | length != 0 %}
                    AND LOWER(table_name) IN ( 
                        {%- for include_table in include_tables -%} '{{ include_table.lower() }}' {%- if not loop.last -%} , {% endif -%} {%- endfor -%} )
                {% else %}
                    AND 1 = 1
                {% endif %}

        {% endset %}

        {% if execute %}
            {% set information_schema_datas = run_query(read_information_schema_datas) %}
        {% endif %}

        {% for information_schema_data in information_schema_datas %}

            {% set source_table_name = information_schema_data[0] + '.' + information_schema_data[1] + '.' + information_schema_data[2] %}
            {% set column_query %}

                SELECT

                    column_name
                    , data_type

                FROM {{ source_database }}.information_schema.columns
                WHERE table_name = '{{ information_schema_data[2] }}' 
                    AND table_schema  = '{{ information_schema_data[1] }}' 
                    AND table_catalog = '{{ information_schema_data[0] }}'
                    
            {% endset %}

            {% if execute %}
                {% set source_columns = run_query(column_query) | list %}
            {% endif %}

            {% set chunk_columns = [] %}

            {% for source_column in source_columns %}

                    {% do chunk_columns.append(source_column) %}
                    {% if (chunk_columns | length) == 100 %}

                        {% set insert_rows %}

                            INSERT INTO {{ target_database }}.{{ target_schema }}.{{ target_table }} 
                            (
                            {% for chunk_column in chunk_columns %}
                                {{ data_profiler.do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                {% if not loop.last %} UNION ALL {% endif %}
                            {% endfor %}
                            )

                        {% endset %}

                        {% do run_query(insert_rows) %}
                        {% do chunk_columns.clear() %}

                    {% endif %}

            {% endfor %}

            {% if (chunk_columns | length) != 0 %}

                {% set insert_rows %}

                    INSERT INTO {{ target_database }}.{{ target_schema }}.{{ target_table }} 
                    (
                    {% for chunk_column in chunk_columns %}
                        {{ data_profiler.do_data_profiling(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                        {% if not loop.last %} UNION ALL {% endif %}
                    {% endfor %}
                    )

                {% endset %}

                {% do run_query(insert_rows) %}
                {% do chunk_columns.clear() %}

            {% endif %}

        {% endfor %}

    {% endif %}

SELECT 'TEMP_STORAGE' AS temp_column

{% endmacro %}