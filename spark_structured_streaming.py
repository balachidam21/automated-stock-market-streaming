import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType
from pyspark.sql import functions as f


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_strucutred_streaming")

def create_spark_session():
    """
    Create a Spark Session with cassandra connectors attached to the Spark cluster.
    """
    try:
        spark = SparkSession \
                .builder \
                .appName("SparkStructuredStreaming") \
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0, org.apache.spark:spark-sql-kafka-0-10_2:12:3.0.0") \
                .config("spark.cassandra.connection.host", "cassandra") \
                .config("spark.cassandra.connection.port", "9042") \
                .config("spark.cassandra.auth.username", "cassandra") \
                .config("spark.cassandra.auth.password", "cassandra") \
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark Session created successfully')
    except Exception:
        logging.error("Could not create the spark session")

    return spark

def connect_kafka_stream(spark):
    """
    Connects with kafka connection to retrieve streaming data to create intial spark dataframe.
    """

    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:19092,kafka:19093,kafka:19094") \
            .option("subscribe", "stock_market") \
            .option("delimiter", ",") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka stream Connection Established")
    except Exception as e:
        logging.warning(f"Kafka Connection could not be established due to exception: {e}")

    return df

def create_schema(df, spark):
    """
    Creates a schema for the streaming data and modifies the initial dataframe
    """
    schema = StructType([
        StructField("key", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("datetime", TimestampType(), False),
        StructField("open", FloatType(), False),
        StructField("close", FloatType(), False),
        StructField("current", FloatType(), False),
        StructField("volume", FloatType(), False)
    ])

    df = df.selectExpr("CAST(value as STRING)").select(f.from_json(f.col("value"), schema).alias("data")).select("data.*")
    return df

def start_streaming(df):
    """
    Starts to stream data from spark dataframe to spark_structured_streaming.stock_market in cassandra
    """
    logging.info("Streaming Started!")
    query = (df.writeStream
             .format("org.apache.spark.sql.cassandra")
             .outputMode("append")
             .options(table="stock_market", keyspace="spark_structured_streaming")
             .start())
    
    return query.awaitTermination()

def write_stream_data():
    """
    Write Streaming data to cassandra table
    """
    spark = create_spark_session()
    df = connect_kafka_stream(spark)
    df_final = create_schema(df, spark)
    start_streaming(df_final)


if __name__ == "__main__":
    write_stream_data()