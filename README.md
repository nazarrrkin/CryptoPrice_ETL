# Realtime cryptocurrency prices streaming

## Intoduction
This project provides a full ETL pipeline for streamig cryptocurrency prices. The hidden api of the crypto exchange are used as data sources.

## System architecture
![](https://github.com/user-attachments/assets/b0bd09d6-9dc2-4e15-a7e6-f3f370aa5b7f)

## Technologies
- Data Source: The hidden api of the crypto exchange.
- Apache Airflow: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- Apache Kafka and Zookeeper: Used for streaming data from PostgreSQL to the processing engine.
- Control Center and Schema Registry: Helps in monitoring and schema management of Kafka streams.
- Apache Spark: For streaming data from kafka to database.
- Clickhouse: Where the processed data will be stored.
- Docker: containerized for ease of deployment and scalability.
