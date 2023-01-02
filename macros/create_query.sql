----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Macro for identifying the data platforms and based on that it will redirect to specific macro
----------------------------------------------------------------------------------------------------------------------------------------------------------
{% macro create_query(target_database, target_schema, target_table) -%}

  {{ return(adapter.dispatch('create_query', 'data_profiler')(target_database, target_schema, target_table)) }}
  
{%- endmacro %}


----------------------------------------------------------------------------------------------------------------------------------------------------------
-- This macro is used to creating the schema and table in snowflake
----------------------------------------------------------------------------------------------------------------------------------------------------------
{% macro snowflake__create_query(target_database, target_schema, target_table) -%}

    {% set get_current_timestamp %}
        SELECT CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ) AS utc_time_zone
    {% endset %}

    {% if execute %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
    {% endif %}

    {% set create_schema %}
        CREATE SCHEMA IF NOT EXISTS {{ target_database }}.{{ target_schema }}
    {% endset %}

    {% do run_query(create_schema) %}

    {% set create_table %}
        CREATE TABLE IF NOT EXISTS {{ target_database }}.{{ target_schema }}.{{ target_table }} (

            database                    VARCHAR(100)     comment 'Database name'
            , schema                    VARCHAR(100)     comment 'Schema name'
            , table_name                VARCHAR(100)     comment 'Name of the table'
            , column_name               VARCHAR(500)     comment 'Name of the column'
            , data_type                 VARCHAR(100)     comment 'Data type of the column'
            , row_count                 NUMBER(38, 0)    comment 'Column based row count'
            , not_null_count            NUMBER(38, 0)    comment 'Count of the not_null values based on columns'
            , not_null_percentage       NUMBER(38, 2)    comment 'Percentage of column values that are not NULL (e.g., 0.62 means that 62% of the values are populated while 38% are NULL)'
            , null_count                NUMBER(38, 0)    comment 'Count of the null values based on columns'
            , null_percentage           NUMBER(38, 2)    comment 'Percentage of column values that are NOT_NULL (e.g., 0.55 means that 55% of the values are populated while 45% are NOT_NULL)'
            , distinct_count            NUMBER(38, 0)    comment 'Count of unique column values in the column'
            , distinct_percentage       NUMBER(38, 2)    comment 'Percentage of unique column values (e.g., 1 means that 100% of the values are unique)'
            , is_unique                 BOOLEAN          comment 'True if all column values are unique'
            , min                       VARCHAR(250)     comment 'Minimum column value'
            , max                       VARCHAR(250)     comment 'Maximum column value'
            , avg                       NUMBER(38, 2)    comment 'Average column value'
            , profiled_at               TIMESTAMP_NTZ(9) comment 'Date and time (UTC time zone) of the profiling'

        )
    {% endset %}

    {% do run_query(create_table) %}

    {{ return(profiled_at) }}

{%- endmacro %}


----------------------------------------------------------------------------------------------------------------------------------------------------------
-- This macro is used to creating the table for postgres and also this is the default macro
----------------------------------------------------------------------------------------------------------------------------------------------------------
{% macro default__create_query(target_database, target_schema, target_table) -%}

    {% set get_current_timestamp %}
        SELECT CAST(NOW() AT TIME ZONE 'UTC'AS TIMESTAMPTZ) AS utc_time_zone
    {% endset %}

    {% if execute %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
    {% endif %}

    {% set create_schema %}
        CREATE SCHEMA IF NOT EXISTS {{ target_schema }}
    {% endset %}

    {% do run_query(create_schema) %}

    {% set create_table %}
        CREATE TABLE IF NOT EXISTS {{ target_database }}.{{ target_schema }}.{{ target_table }} (

            database                    VARCHAR(100)
            , schema                    VARCHAR(100)
            , table_name                VARCHAR(100)
            , column_name               VARCHAR(500)
            , data_type                 VARCHAR(100)
            , row_count                 NUMERIC(38, 0)
            , not_null_count            NUMERIC(38, 0)
            , not_null_percentage       NUMERIC(38, 2)
            , null_count                NUMERIC(38, 0)
            , null_percentage           NUMERIC(38, 2)
            , distinct_count            NUMERIC(38, 0)
            , distinct_percentage       NUMERIC(38, 2)
            , is_unique                 BOOLEAN
            , min                       VARCHAR(250)
            , max                       VARCHAR(250)
            , avg                       NUMERIC(38, 2)
            , profiled_at               TIMESTAMPTZ(9)
        );

        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.database is 'Database name';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.schema is 'Schema name';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.table_name is 'Name of the table';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.column_name is 'Name of the column';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.data_type is 'Data type of the column';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.row_count is 'Column based row count';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.not_null_count is 'Count of the not_null values based on columns';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.not_null_percentage is 'Percentage of column values that are not NULL (e.g., 0.62 means that 62% of the values are populated while 38% are NULL)';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.null_count is 'Count of the null values based on columns';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.null_percentage is 'Percentage of column values that are NOT_NULL (e.g., 0.55 means that 55% of the values are populated while 45% are NOT_NULL)';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.distinct_count is 'Count of unique column values in the column';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.distinct_percentage is 'Percentage of unique column values (e.g., 1 means that 100% of the values are unique)';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.is_unique is 'True if all column values are unique';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.min is 'Minimum column value';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.max is 'Maximum column value';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.avg is 'Average column value';
        COMMENT ON COLUMN {{ target_database }}.{{ target_schema }}.{{ target_table }}.profiled_at is 'Date and time (UTC time zone) of the profiling';

    {% endset %}

    {% do run_query(create_table) %}

    {{ return(profiled_at) }}

{%- endmacro %}