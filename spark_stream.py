import logging 
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, when, hour, dayofweek
from pyspark.sql.types import StructType, StructField, StringType
import os
from pathlib import Path
import urllib.request


def setup_hadoop_binaries():
    """Download and setup Hadoop binaries for Windows"""
    hadoop_dir = Path("C:/hadoop")
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    # Download winutils.exe if not present
    winutils_path = bin_dir / "winutils.exe"
    if not winutils_path.exists():
        print("Downloading winutils.exe...")
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
        urllib.request.urlretrieve(winutils_url, str(winutils_path))
    
    # Download hadoop.dll if not present
    hadoop_dll_path = bin_dir / "hadoop.dll"
    if not hadoop_dll_path.exists():
        print("Downloading hadoop.dll...")
        hadoop_dll_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
        urllib.request.urlretrieve(hadoop_dll_url, str(hadoop_dll_path))
    
    # Set environment variables
    os.environ['HADOOP_HOME'] = str(hadoop_dir)
    os.environ['PATH'] = f"{str(bin_dir)};{os.environ['PATH']}"
    print("Hadoop binaries setup completed")


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS energy_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        print("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")


def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS energy_data.consumption_records (
                id UUID PRIMARY KEY,
                datetime TIMESTAMP,
                global_active_power DOUBLE,
                global_reactive_power DOUBLE,
                voltage DOUBLE,
                global_intensity DOUBLE,
                sub_metering_1 DOUBLE,
                sub_metering_2 DOUBLE,
                sub_metering_3 DOUBLE,
                active_energy_other DOUBLE
            );
        """)
        print("Table created successfully!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")


def insert_data(session, record):
    """
    Inserts data into the Cassandra table.
    """
    try:
        session.execute("""
            INSERT INTO energy_data.consumption_records (id, datetime, global_active_power, global_reactive_power, 
                voltage, global_intensity, sub_metering_1, sub_metering_2, sub_metering_3, active_energy_other)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uuid.uuid4(), record['datetime'], record['global_active_power'],
              record['global_reactive_power'], record['voltage'], record['global_intensity'],
              record['sub_metering_1'], record['sub_metering_2'], record['sub_metering_3'],
              record['active_energy_other']))
        logging.info(f"Data inserted for record on {record['datetime']}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


# def create_spark_connection():
#     """Creates a SparkSession with necessary configurations"""
#     try:
#         setup_hadoop_binaries()  # Setup Hadoop before creating Spark session
#         spark = SparkSession.builder \
#             .appName('EnergyConsumptionStreaming') \
#             .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1," 
#                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
#             .config('spark.cassandra.connection.host', 'localhost') \
#             .master("local[*]") \
#             .getOrCreate()

#         spark.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#         return spark
#     except Exception as e:
#         logging.error(f"Couldn't create Spark session: {e}")
#         return None

def create_spark_connection():
    """Creates a SparkSession with necessary configurations"""
    try:
        setup_hadoop_binaries()  # Setup Hadoop before creating Spark session
        
        spark = SparkSession.builder \
            .appName('EnergyConsumptionStreaming') \
            .config('spark.jars.packages', 
                    "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.jars', '/opt/bitnami/spark/jars/scala-library-2.13.15.jar') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .master("local[*]") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create Spark session: {e}")
        return None


def connect_to_kafka(spark_conn, topic_name):
    """
    Connects to Kafka and reads from the given topic.
    """
    try:
        kafka_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', topic_name) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return kafka_df
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created: {e}")
        return None


def create_selection_df(kafka_df):
    """
    Extracts and formats Kafka message data into a Spark DataFrame.
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("global_active_power", StringType(), True),
        StructField("global_reactive_power", StringType(), True),
        StructField("voltage", StringType(), True),
        StructField("global_intensity", StringType(), True),
        StructField("sub_metering_1", StringType(), True),
        StructField("sub_metering_2", StringType(), True),
        StructField("sub_metering_3", StringType(), True)
    ])

    formatted_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return formatted_df


def preprocess_data(df):
    """
    Perform preprocessing on the DataFrame:
    - Forward/backward fill missing values.
    - Combine Date and Time into a datetime column.
    - Feature engineering (additional energy metrics, time-related features).
    - Categorical encoding of time of day.
    """
    # Handle missing values by forward filling
    df = df.fillna(method="ffill")

    # Combine Date and Time into datetime column
    df = df.withColumn("datetime", to_timestamp(col("date") + " " + col("time"), "dd/MM/yyyy HH:mm:ss"))

    # Convert columns to appropriate types
    df = df.withColumn("global_active_power", col("global_active_power").cast("double")) \
           .withColumn("global_reactive_power", col("global_reactive_power").cast("double")) \
           .withColumn("voltage", col("voltage").cast("double")) \
           .withColumn("global_intensity", col("global_intensity").cast("double")) \
           .withColumn("sub_metering_1", col("sub_metering_1").cast("double")) \
           .withColumn("sub_metering_2", col("sub_metering_2").cast("double")) \
           .withColumn("sub_metering_3", col("sub_metering_3").cast("double"))

    # Calculate active energy consumed by unmetered equipment
    df = df.withColumn("active_energy_other", (col("global_active_power") * 1000 / 60) - col("sub_metering_1") - col("sub_metering_2") - col("sub_metering_3"))

    # Time-related features
    df = df.withColumn("hour", hour(col("datetime"))) \
           .withColumn("day_of_week", dayofweek(col("datetime")))

    # Categorical encoding for Time of Day (morning, afternoon, evening, night)
    df = df.withColumn("time_of_day", 
                       when(col("hour").between(6, 9), "morning")
                       .when(col("hour").between(12, 17), "afternoon")
                       .when(col("hour").between(18, 21), "evening")
                       .otherwise("night"))

    return df


def create_cassandra_connection():
    """
    Connects to the Cassandra cluster.
    """
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None


if __name__ == "__main__":
    topic = "energyConsumptionRecords"

    # Initialize Spark connection
    spark_conn = create_spark_connection()
    print("*************************************DONE*******************************")

    if spark_conn:
        # Connect to Kafka
        kafka_df = connect_to_kafka(spark_conn, 'energyConsumptionRecords')
        print("*************************************DONE*******************************")

        if kafka_df:
            # Format incoming Kafka data
            formatted_df = create_selection_df(kafka_df)
            print("*************************************DONE*******************************")

            # Preprocess the data
            processed_df = preprocess_data(formatted_df)
            print("*************************************DONE*******************************")

            # Connect to Cassandra
            cassandra_session = create_cassandra_connection()
            print("*************************************DONE*******************************")

            if cassandra_session:
                # Create keyspace and table
                create_keyspace(cassandra_session)
                print("*************************************DONE*******************************")
                create_table(cassandra_session)
                print("*************************************DONE*******************************")

                # Write data to Cassandra using Structured Streaming
                streaming_query = (processed_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option("checkpointLocation", "/tmp/checkpoint")  # Change to persistent path if needed
                                   .option("keyspace", "energy_data")
                                   .option("table", "consumption_records")
                                   .start())

                # Await termination of the streaming job
                streaming_query.awaitTermination()
