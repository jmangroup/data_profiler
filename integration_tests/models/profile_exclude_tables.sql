-- Pass one or many exclude_tables in the parameter.It will profile all the tables except the exclude tables.
{{
    config(
        tags=["test_model"]
    )
}}

{{ data_profiler.data_profiling(target_database = 'profiling_test'
                                    , target_schema = 'profiling'
                                    , target_table = 'data_profile_exclude_table'
                                    , source_database = 'profiling_test'
                                    , source_schema = ['integration_tests_customer_detail', 'integration_tests_order_detail']
                                    , exclude_tables = ['address']) }}