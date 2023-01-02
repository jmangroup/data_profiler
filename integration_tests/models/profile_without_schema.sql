-- Pass one or many source_schema in the parameter.It will profile only all the tables in the databse except information schema and pg_catalog schema.
{{
    config(
        tags=["test_model"]
    )
}}


{{ data_profiler.data_profiling(target_database = 'profiling_test'
                                    , target_schema = 'profiling'
                                    , target_table = 'data_profile_without_schema'
                                    , source_database = 'profiling_test') }}