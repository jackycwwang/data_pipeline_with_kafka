import os
from datetime import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import logging

import pandas as pd


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

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
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


# Define Kafka configuration
# TODO Change the following configurations
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'E3IAKOQ2TDCI3T7P',
    'sasl.password': 'C2ISLqGvNcJWcuaCZIs+UoypNWIhs4cFh9zyVmPS1X5VZ5R52A0oqRIASt2MVbtb'
}

# Create a Schema Registry client
# TODO Change the following configurations
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-mw0d1.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('6363DF6F5TXQHQQH', 'nTNjN741JOsc1ye0Ei/t2XNblr9U0fkTiA2nSaj3k/SS667NJQGXAAcQpSfzEaej')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print('schema_str:',schema_str)

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)


# Define the SerializingProducer
try:
    producer = SerializingProducer({
            'bootstrap.servers': kafka_config['bootstrap.servers'],
            'security.protocol': kafka_config['security.protocol'],
            'sasl.mechanisms': kafka_config['sasl.mechanisms'],
            'sasl.username': kafka_config['sasl.username'],
            'sasl.password': kafka_config['sasl.password'],
            'key.serializer': key_serializer,  # Key will be serialized as a string
            'value.serializer': avro_serializer  # Value will be serialized as Avro
        }
    )
except Exception as err:
    logger.error(f"Kafka cluster connection error: {err}")

# ------------------------------------------------------------------------

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host_name =  os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# Establish a connection









