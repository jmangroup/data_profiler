FROM postgres:14-bullseye
ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=test
ENV POSTGRES_DB=profiling_test
COPY  . .
RUN apt-get update && apt-get install -y python3-pip && apt-get install -y git
RUN pip install -r requirements.txt
WORKDIR /integration_tests
RUN python3 profiles_yml_creator.py