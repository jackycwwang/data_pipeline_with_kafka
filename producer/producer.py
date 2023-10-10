import logging
import json
import os
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
from datetime import datetime
import pytz

# import pandas as pd

import mysql.connector

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import hydra
from omegaconf import OmegaConf, DictConfig

logger = logging.getLogger("my_logger")

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to key {}, partition [{}], at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


@hydra.main(version_base=None, config_path='conf', config_name='config')
def producer_app(cfg: DictConfig) -> None:
    sr_cfg = cfg['schema_registry']
    kafka_cfg = cfg['kafka']

    # ----------- Make the connections to Kafka cluster and Schema Registry -----------
    # Create a Schema Registry client
    schema_registry_config = {
        'url': sr_cfg['url'],
        'basic.auth.user.info': '{}:{}'.format(sr_cfg['user'], sr_cfg['secret'])
    }

    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    # Fetch the latest Avro schema for the value
    subject_name = 'product_updates-value'
    schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
    # print('schema_str:',schema_str)

    # Create Avro Serializer for the value
    key_serializer = StringSerializer('utf_8')
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    # Define Kafka configuration and create a kafka producer
    kafka_config = {
        'bootstrap.servers': kafka_cfg['bootstrap_servers'],
        'sasl.mechanisms': kafka_cfg['sasl_mechanisms'],
        'security.protocol': kafka_cfg['security_protocol'],
        'sasl.username': kafka_cfg['sasl_username'],
        'sasl.password': kafka_cfg['sasl_password'],
        'key.serializer': key_serializer,
        'value.serializer': avro_serializer
    }
    try:
        producer = SerializingProducer(kafka_config)
    except Exception as err:
        logger.error(f"Kafka cluster connection error: {err}")


    # ---------- Read the timestamp that the data was last fetched from the database ---------
    filename = "time_track/last_read_timestamp.json"
    with open(filename, "r") as fp:
        try:
            last_read = json.load(fp)
        except json.JSONDecodeError:
            last_read = {'last_read_timestamp': None}

    last_read_timestamp = last_read.get('last_read_timestamp', None)
    # if this is the first time, we initialize the last_read_timestamp
    # and save it to the json file
    if last_read_timestamp is None:
        with open(filename, 'w') as fp:
            last_read_timestamp = '2018-01-01 00:00:00'
            last_read['last_read_timestamp'] = last_read_timestamp
            json.dump(last_read, fp)

    # -------- Define your database connection parameters ----------

    # settings for docker compose
    db_config = {
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASSWORD'),
        "host": os.getenv('DB_HOST'),
        "port": int(os.getenv('DB_PORT')),
        "database": os.getenv('DB_NAME'),
    }

    # settings for testing
    # db_config = {
    #     "user": "mysql",
    #     "password": "mysql",
    #     "host": "172.25.184.208",
    #     "port": 3307,
    #     "database": "buy_online_db",
    # }

    # Establish a connection to the database
    connection = mysql.connector.connect(**db_config)

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    # Define the timezone for the datetime objects
    tz = pytz.timezone('America/New_York')
    last_read_obj = tz.localize(datetime.strptime(last_read_timestamp, '%Y-%m-%d %H:%M:%S'))
    # last_read_obj = datetime.strptime(last_read_timestamp, '%Y-%m-%d %H:%M:%S')

    # ----------- Query the database and produce to the kafka cluster topic ----------
    # Execute the SQL query
    sql_query = """
        SELECT id, name, category, price, last_updated
        FROM product
        WHERE last_updated > %s
        limit 10
    """
    try:
        while True:
            cursor.execute(sql_query, (last_read_obj,))

            # Fetch all rows from the result set
            rows = cursor.fetchall()
            logger.info(f"----- Fetch row count: {cursor.rowcount} -----")

            # Iterate through the rows and print the values
            if rows:
                for row in rows:
                    id, name, category, price, last_updated = row
                    last_updated = last_updated.astimezone(tz)
                    logger.info(f"ID: {id}, Name: {name}, Category: {category}, Price: {price}, Last Updated: {last_updated}")

                    value = {
                        'id': id,
                        'name': name,
                        'category': category,
                        'price': price,
                        'last_updated': last_updated.strftime('%Y-%m-%d %H:%M:%S')
                    }

                    producer.produce(topic='product_updates',
                                    key=str(id),
                                    value=value,
                                    on_delivery=delivery_report)
                    producer.flush()  # may become blocking in a long run
                    # sleep(10)

                    # find the maximum timestamp in this batch
                    if last_updated > last_read_obj:
                        last_read_obj = last_updated

                    with open(filename, 'w') as fp:
                        last_read['last_read_timestamp'] = last_read_obj.strftime('%Y-%m-%d %H:%M:%S')
                        json.dump(last_read, fp)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and the database connection
        cursor.close()
        connection.close()

if __name__ == "__main__":
    producer_app()