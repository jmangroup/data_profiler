# Overview
* [Prerequisites](#Prerequisites)
* [Configure](#Configure)
* [Build](#Build)
* [Run](#Run)


## Prerequisites
- python3
- Docker

## Configure
Create the env file for your TARGET in `integration_tests/.env`. For the postgres, default values are `user=root`, `password=test` and `database=profiling_test`.

## Build

Docker and `dockerfile` were used for testing. Specific instructions for your OS can be found [here](https://docs.docker.com/get-docker/).

No set up required for this test. All test procedures are declared in the docker file. Use the command below to create a docker image.

```shell
docker build -t <image name> .
```

## Run

Use the following command to launch the docker image after it has been created.

```shell
docker run <image name>
```

Run the following command in the Docker container CLI after that, or open a new terminal and type `docker exec -it <container id> bash` before doing so.

```shell
dbt debug --target <target name>
dbt deps
dbt seed --target <target name> --full-refresh
dbt run --target <target name>
dbt test --target <target name>
```

If all the tests were pass, then you're good to go! 

When PR is created against this repo, all tests will run automatically 