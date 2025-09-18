FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

USER airflow
