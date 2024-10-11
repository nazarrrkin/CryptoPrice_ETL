from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col
import logging
import clickhouse_connect


class Streaming:
    def __init__(self, appname='CreeptStreaming', kafka_bootstrap_servers='127.22.224.1:9092'):
        self.appname = appname
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

    def create_spark_connection(self):
        sp_con = None
        try:
            sp_con = SparkSession.builder \
                .appname(self.appname) \
                .config('spark.jars.packages', 'com.clickhouse:clickhouse-jdbc:0.3.2', \
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
                .config('spark.clickhouse.url', 'jdbc:clickhouse://localhost:8123/default') \
                .config('spark.clickhouse.user', 'admin') \
                .config('spark.clickhouse.password', 'admin') \
                .getOrCreate()

            sp_con.sparkContext.setLogLevel("ERROR")
            logging.info("Spark connection created successfully")

        except Exception as e:
            logging.warning(f'Couldnt create spark connection due to {e}')

        return sp_con

    def connection_to_kafka(self):
        creept_df = None
        try:
            df = sp_con.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', self.kafka_bootstrap_servers) \
                .option('subscribe', 'creept') \
                .optin('startingOffsets', 'earliest') \
                .load()
            logging.info('Kafka dataframe has been created')
        except Exception as e:
            logging.warning(f'Couldnt create kafka dataframe due to {e}')
        return df

    def df_selection_from_kafka(selfdf):
        schema = StructType([
            StructField('name', StringType(), True),
            StructField('symbol', StringType(), True),
            StructField('circulatingSupply', StringType(), True),
            StructField('price', IntegerType(), True),
            StructField('volume24h', IntegerType(), True),
            StructField('marketCap', IntegerType(), True),
            StructField('percentChange24h', IntegerType(), True),
            StructField('date', TimestampType(), True)
        ])

        struct_df = df.selectExpr('CAST(value AS STRING)') \
            .select(from_json(col('value', schema).alias('data')).select('data.*'))
        return struct_df


class Database:
    def __init__(self, host='localhost', port=8123, username='admin', password='admin') -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = clickhouse_connect.get_client(host, port, username, password)

    def initialize(self):
        create_table = """
        CREATE TABLE IF NOT EXISTS creept (
            name String,
            symbol String,
            circulatingSupply String,
            price Int32,
            volume24h Int32,
            marketCap Int32,
            percentChange24h Int32,
            date DateTime
        ) ENGINE  = MergeTree()
        """
        self.client.command(create_table)
        logging.info('Clickhouse table has been created')

    def add_data_connect(self, struct_df):
        insert = """
        INSERT INTO creept (name, symbol, circelatingSupply, price, volume24h, marketCap, percentChange24h, date) VALUES
        """
        values = ','.join(f"('{raw['name']}', '{raw['symbol']}', '{raw['circelatingSupply']}', '{raw['price']}', '{raw['volume24h']}',
                        '{raw['marketCap']}', '{raw['percentChange24h']}', '{raw['date']}')" for raw in struct_df.collect())
        self.client.command(insert+values)
        logging.info('Data has been inserted')

    def add_data_jdbc(self, df, epoch_id):
        df.write \
            .format('jdbc') \
            .option('url', 'jdbc:clickhouse://localhost:8123/default') \
            .option("driver", 'ru.yandex.clickhouse.ClickHouseDriver') \
            .option('creept', 'creept') \
            .option('user', 'admin') \
            .option('password', 'admin') \
            .mode('append') \
            .save()
        logging.info('Data has been inserted')


def data_streaming():
    clickhouse = Database()
    streaming = Streaming()
    clickhouse.initialize()
    sp_con = streaming.create_spark_connection()
    if sp_con:
        df = streaming.connection_to_kafka(sp_con)
        if df:
            struct_df = streaming.df_selection_from_kafka(df)
            stream = struct_df.writeStream \
                .foreachBatch(lambda batch: clickhouse.add_data_jdbc(batch)) \
                .outputMode('update') \
                .start()
            stream.awaitTermination()
