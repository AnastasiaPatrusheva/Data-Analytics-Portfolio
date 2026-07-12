# Airflow с Java + PySpark + клиентом ClickHouse — чтобы задачи DAG запускали Spark внутри контейнера
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 clickhouse-connect==0.7.19 pyarrow==16.1.0 pytest==8.2.2
