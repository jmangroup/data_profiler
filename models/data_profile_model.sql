{{
    config(
        alias='TMP_DATA_PROFILE',
        materialized='table' if target.name == 'postgres' else 'ephemeral',
        post_hook= 'DROP TABLE IF EXISTS {{ this }}'

    )
}}

{% set empty_columns_query %}
    {% if target.name == 'postgres' %}
        SELECT *
        FROM "postgres_test"."test"."config_data_profile"
        WHERE destination_database IS NULL 
            OR destination_schema IS NULL
            OR destination_table IS NULL
            OR source_database IS NULL;
    {% else %}
        SELECT *
        FROM GOVERNANCE_DEV.DATA_META.CONFIG_DATA_PROFILE
        WHERE destination_database IS NULL 
            OR destination_schema IS NULL
            OR destination_table IS NULL
            OR source_database IS NULL;
    {% endif %}
{% endset %}

{% set empty_columns_result = run_query(empty_columns_query) %}
{% if empty_columns_result | length > 0 %}
        {% set error_message = "Validation Error: Empty column(s) found in seed table" %}
        {{ print("\033[91m" ~ error_message ~ "\033[0m") }}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% else %}
    {% if execute %}
        {% set seed_table %}
            {% if target.name == 'postgres' %}
                "postgres_test"."test"."config_data_profile"
            {% else %}
                GOVERNANCE_DEV.DATA_META.CONFIG_DATA_PROFILE
            {% endif %}
        {% endset %}

        {% set get_current_timestamp %}
            {% if target.name == 'postgres' %}
                SELECT current_timestamp AT TIME ZONE 'UTC' AS utc_time_zone;
            {% else %}
                SELECT CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS utc_time_zone;
            {% endif %}
        {% endset %}

        {% set read_config_table = run_query('SELECT * FROM ' ~ seed_table) %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}

            {% for profile_detail in read_config_table %}
                {% set destination_database = profile_detail[0] %}
                {% set destination_schema = profile_detail[1] %}
                {% set destination_table = profile_detail[2] %}
                {% set source_database = profile_detail[3] %}

                {% if target.name == 'postgres' %}
                
                    {% set create_schema_sql %}
                        {{ create_new_schema_postgres(destination_schema) }}
                    {% endset %}

                    {% set create_schema_statement = run_query(create_schema_sql) %}
                    {% if create_schema_statement.rows[0].create_schema_statement %}
                        {% do run_query(create_schema_statement.rows[0].create_schema_statement) %}
                    {% endif %}


                    {% set create_table %}
                        {{ create_data_profile_table_postgres(destination_database, destination_schema, destination_table) }}
                    {% endset %}
                    {% do run_query(create_table) %}

                    {% set include_schemas = profile_detail[4].split(',') if profile_detail[4] is not none else [] %}
                    {% set exclude_schemas = profile_detail[5].split(',') if profile_detail[5] is not none else [] %}
                    {% set include_tables = profile_detail[6].split(',') if profile_detail[6] is not none else [] %}
                    {% set exclude_tables = profile_detail[7].split(',') if profile_detail[7] is not none else [] %}

                    {% set read_information_schema_datas %}
                        {{ read_information_schema_postgres(source_database, include_schemas, exclude_schemas, include_tables, exclude_tables) }}
                    {% endset %}
                    {% set information_schema_datas = run_query(read_information_schema_datas) %}
                    {% set filtered_information_schema_datas = information_schema_datas | selectattr(2, 'ne', destination_table) | list %}

                    {% for information_schema_data in filtered_information_schema_datas %}
                        {% set source_table_name = information_schema_data[0] ~ '.' ~ information_schema_data[1] ~ '.' ~ information_schema_data[2] %}
                        {% set source_columns_query = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'' ~ information_schema_data[2] ~ '\' AND table_schema = \'' ~ information_schema_data[1] ~ '\' AND table_catalog = \'' ~ information_schema_data[0] ~ '\'' %}
                        {% set source_columns = run_query(source_columns_query) %}
                        {% set chunk_columns = [] %}

                        {% for source_column in source_columns %}
                            {% do chunk_columns.append(source_column) %}
                            {% if loop.index % 100 == 0 or loop.last %}
                                {% set data_profile_queries = [] %}
                                {% for chunk_column in chunk_columns %}
                                    {% set data_profile_query = do_data_profile_postgres(information_schema_data, source_table_name, chunk_column, profiled_at) %}
                                    {% do data_profile_queries.append(data_profile_query) %}
                                {% endfor %}

                                {% set insert_rows %}
                                    INSERT INTO {{ destination_database }}.{{ destination_schema }}.{{ destination_table }} (
                                        {% for query in data_profile_queries %}
                                            {{ query }}{% if not loop.last %} UNION ALL {% endif %}
                                        {% endfor %}
                                    )                                
                                {% endset %}
                                {% do run_query(insert_rows) %}
                                {% set chunk_columns = [] %}
                            {% endif %}
                        {% endfor %}
                    {% endfor %}
                {% else %}
                    CREATE OR REPLACE TEMP TABLE temp_table AS
                    SELECT * FROM {{ destination_database }}.{{ destination_schema }}.{{ destination_table }};

                    {% set db_create %}
                        {{ create_new_database_if_not_exists(destination_database) }}
                    {% endset %}
                    {% do run_query(db_create) %}

                    {% set schema_create %}
                        {{ create_new_schema(destination_database, destination_schema) }}
                    {% endset %}
                    {% do run_query(schema_create) %}

                    {% set create_table %}
                        {{ create_data_profile_table(destination_database, destination_schema, destination_table) }}
                    {% endset %}
                    {% do run_query(create_table) %}

                    {% set include_schemas = profile_detail[4].split(',') if profile_detail[4] is not none else [] %}
                    {% set exclude_schemas = profile_detail[5].split(',') if profile_detail[5] is not none else [] %}
                    {% set include_tables = profile_detail[6].split(',') if profile_detail[6] is not none else [] %}
                    {% set exclude_tables = profile_detail[7].split(',') if profile_detail[7] is not none else [] %}

                    {% set read_information_schema_datas %}
                        {{ read_information_schema(source_database, include_schemas, exclude_schemas, include_tables, exclude_tables) }}
                    {% endset %}
                    {% set information_schema_datas = run_query(read_information_schema_datas) %}
                    {% set filtered_information_schema_datas = information_schema_datas | selectattr(2, 'ne', destination_table) | list %}

                    {% for information_schema_data in filtered_information_schema_datas %}
                        {% set source_table_name = information_schema_data[0] ~ '.' ~ information_schema_data[1] ~ '.' ~ information_schema_data[2] %}
                        {% set source_columns = adapter.get_columns_in_relation(source_table_name) | list %}
                        {% set chunk_columns = [] %}

                        {% set validator_query %}
                            SELECT 
                                'Query ID :' || query_id || ';\n' || ERROR_MESSAGE AS error_info
                            FROM TABLE({{ destination_database }}.information_schema.query_history())
                            WHERE 
                                session_id = CURRENT_SESSION()
                                AND execution_status LIKE 'FAILED%';
                        {% endset %}

                        {% for source_column in source_columns %}
                            {% do chunk_columns.append(source_column) %}
                            {% if (chunk_columns | length) == 100 %}
                                {% set insert_rows %}
                                    INSERT INTO {{ destination_database }}.{{ destination_schema }}.{{ destination_table }} (
                                        {% for chunk_column in chunk_columns %}
                                            {{ do_data_profile(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                            {% if not loop.last %} UNION ALL {% endif %}
                                        {% endfor %}
                                    )
                                {% endset %}
                                {% do run_query(insert_rows) %}
                                {% set validator_results = run_query(validator_query) %}
                                {% if validator_results | length > 0 %}
                                    {{ exceptions.raise_compiler_error(validator_results.columns[0].values()[0]) }}
                                {% endif %}
                                {% do chunk_columns.clear() %}
                            {% endif %}
                        {% endfor %}

                        {% if (chunk_columns | length) != 0 %}
                            {% set insert_rows %}
                                INSERT INTO {{ destination_database }}.{{ destination_schema }}.{{ destination_table }} (
                                    {% for chunk_column in chunk_columns %}
                                        {{ do_data_profile(information_schema_data, source_table_name, chunk_column, profiled_at) }}
                                        {% if not loop.last %} UNION ALL {% endif %}
                                    {% endfor %}
                                )
                            {% endset %}
                            {% do run_query(insert_rows) %}
                            {% set validator_results = run_query(validator_query) %}
                            {% if validator_results | length > 0 %}
                                {{ exceptions.raise_compiler_error(validator_results.columns[0].values()[0]) }}
                            {% endif %}
                            {% do chunk_columns.clear() %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            {% endfor %}
    {% endif %}
{% endif %}
-- This select statement is used to temporarily store the data in the table after that, we delete this table using the post hook method 
SELECT 'temp_data_for_creating_the_table' AS temp_column
