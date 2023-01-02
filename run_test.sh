cd integration_tests
# echo $ENV_FILE| sed 's/ /\n/g' > .env
# cat .env
python profiles_yml_creator.py
cat profiles.yml
dbt debug --target $1
dbt deps --target $1
dbt seed --target $1 --full-refresh
dbt run -m tag:test_model --target $1
dbt run -m tag:test_cases --target $1
dbt test --target $1