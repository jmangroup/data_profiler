{{
    config(
        tags=["test_cases"]
    )
}}

SELECT 
	 CASE
		WHEN (SELECT COUNT(*) FROM profiling_test.profiling.data_profile_multi_schema) != 0 THEN 1
		ELSE NULL
	END AS multiple_schema_row_count
    , CASE
		WHEN (SELECT COUNT(*) FROM profiling_test.profiling.data_profile_single_schema) != 0 THEN 1
		ELSE NULL
	END AS single_schema_row_count
    , CASE
		WHEN (SELECT COUNT(*) FROM profiling_test.profiling.data_profile_exclude_table) != 0 THEN 1
		ELSE NULL
	END AS exclude_table_row_count
    , CASE
		WHEN (SELECT COUNT(*) FROM profiling_test.profiling.data_profile_include_table) != 0 THEN 1
		ELSE NULL
	END AS include_table_row_count
	, CASE
		WHEN (SELECT COUNT(*) FROM profiling_test.profiling.data_profile_without_schema) != 0 THEN 1
		ELSE NULL
	END AS without_schema_row_count