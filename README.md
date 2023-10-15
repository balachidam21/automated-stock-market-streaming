# Automated Stock Market Streaming
This repository illustrates the use of multiple tech stacks like Apache Spark, Kafka, Airflow and Cassandra.

This project demonstrates how streaming data is extracted, structured and stored using a automated pipeline deployed in Airflow.

Retrives stock market info from Market Stack API. Sends the stock market API data to Kafka topic every 1 hour. Each message is read by Kafka consumer using Spark Structured Streaming and written to Cassandra table every time the data is streamed.

## Apache Airflow

Run the following command to clone the necessary repo on your local

``` bash
git clone https://github.com/balachidam21/docker-airflow.git
```

After cloning the repo, run the following command once
```bash
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
```
This docker command will bind Airflow Container with Kafka and Spark and necessary modules will be installed.

To run Airflow on your Local Machine, run the Local Executor docker-compose file.
```bash
docker-compose -f docker-compose-LocalExecutor.yml up -d
```

The Airflow container is up and running. The Airflow UI can be accessed at `https://localhost:8080`

## Apache Kafka

`docker-compose.yml` will create a multinode Kafka cluster with 3 nodes. You can see the Kafka UI at `localhost:8888`

To build and start container, run the command:
```bash
docker-compose up -d
```

In Kafka UI, create the topic `stock_market` to see the messages coming to Kafka topic.

## Cassandra
`docker-compose.yml` will create a Cassandra server. Every env variable is located in docker-compose file. 

To access the cassandra server, run the command:

```bash
docker exec -it cassandra /bin/bash
```

In the bash terminal in cassandra container, run the command to access cqlsh cli/

```bash
cqlsh -u cassandra -p cassandra
```

Then, we can create keyspace `spark_strucutred_streaming` and table `stock_market` in Cassandra.

```bash
CREATE KEYSPACE spark_structured_streaming WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
```

```bash
CREATE TABLE spark_strcutred_streaming.stock_marke(key text primary key, symbol text, datetime text, open float, close float, current float, volume float);
```

## Spark
Copy the local PySpark script into container:

```bash
docker cp spark_structured_streaming.py spark_master:/opt/bitnami/spark
```

Access the Spark Container and run the following commands to  install necessary JAR files under jars directory to install necessary JAR files under jars directory.

```bash
docker exec -it spark_master /bin/bash
```

```bash
cd jars
curl -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.3.0/spark-sql-kafka-0-10_2.13-3.3.0.jar
```
While the API data is sent to the Kafka topic `random_names` regularly, we can submit the PySpark application and write the topic data to Cassandra table:

```bash
cd ..
spark-submit --master local[2] --jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.3.0.jar,/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar spark_streaming.py
```

After running the commmand, the data is populated into Cassandra table.

## Code Repo and Run Instructions

`streaming.py` -> This script retrieves data from Market Stack API and sends it to Kafka topic.
`stream_dag.py` -> This script containes Airflow DAG that automates to run the pipeline every hour.
`spark_structured_streaming.py` -> This script contains Spark code that acts as Kafka Consumer that consumes streaming data produced by `streaming.py` and structures the data and writes to Cassandra table.

Move the `streaming.py` and `stream_dag.py` scripts under `dags` folder in `docker-airflow` repo. Once you refresh the Airflow UI, `stock_market_streaming` DAG appears in DAGs page.