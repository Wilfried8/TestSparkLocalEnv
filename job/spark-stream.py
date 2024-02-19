import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from kafka import KafkaConsumer
import json


def create_keyspace(session):
    # 3 creation of the key space
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


def create_table(session):
    # 4 create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


def insert_data(session, **kwargs):
    # 5 insert data in the table
    print("inserting data ......")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, 
                address, post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))

        logging.info(f"data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"couldn't insert data due to {e}")


def create_spark_connection():
    # 1 create spark connection
    session_connection = None
    try:
        session_connection = SparkSession.builder \
            .appName("StreamDataFromKafkaToSpark") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()
        session_connection.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    except Exception as e:
        logging.error(f"couldn't create a spark session due to exception : {e}")

    return session_connection


# def create_kafka_consumer(topic_name):
#     # Set up a Kafka consumer with specified topic and configurations
#     consumer = KafkaConsumer(
#         topic_name,
#         bootstrap_servers='localhost:9092',
#         auto_offset_reset='earliest',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
#     return consumer


def create_cassandra_connection():
    # 2 connection to cassandra cluster
    cassandra_session = None
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()

        return cassandra_session
    except Exception as e:
        logging.error(f"couldn't create cassandra connection due to {e}")
        return None


def create_kafka_connection(spark_connection):
    # 6 connection kafka to spark
    # users_created is the topic
    spark_data = None
    try:
        spark_data = spark_connection.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29192") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe couldn't be created due to : {e}")

    # Data with one col "value"
    return spark_data


def create_selected_dataFrame_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False)
        ]
    )

    selection_data = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema=schema).alias('data')).select("data.*")
    selection_data.printSchema()

    return selection_data


if __name__ == "__main__":
    # create spark connection
    # spark_connection = create_spark_connection()
    #
    # if spark_connection is not None:
    #     # connect kafka to spark connection
    #     spark_df = create_kafka_connection(spark_connection=spark_connection)
    #
    #     # aplatir Json to dataframe
    #     selection_df = create_selected_dataFrame_from_kafka(spark_df=spark_df)
    #
    #     # Connection to cassandra
    #     cassandra_session = create_cassandra_connection()
    #
    #     if cassandra_session is not None:
    #         create_keyspace(session=cassandra_session)
    #         create_table(session=cassandra_session)
    #         # insert_data(session=cassandra_session)
    #
    #         logging.info("Streaming is being started...")
    #
    #         streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
    #                            .option('checkpointLocation', '/tmp/checkpoint')
    #                            .option('keyspace', 'spark_streams')
    #                            .option('table', 'created_users')
    #                            .start())
    #
    #         streaming_query.awaitTermination()

    spark_connection = create_spark_connection()

    if spark_connection is not None:
        # connect kafka to spark connection
        spark_df = create_kafka_connection(spark_connection=spark_connection)

        selection_df = create_selected_dataFrame_from_kafka(spark_df=spark_df)

        # Connection to cassandra
        cassandra_session = create_cassandra_connection()

        if cassandra_session is not None:
            create_keyspace(session=cassandra_session)
            create_table(session=cassandra_session)
            # insert_data(session=cassandra_session)

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()