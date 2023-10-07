import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from sqlalchemy import create_engine, text

import pandas as pd


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

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:password@localhost:3306/buy_online_db")

connection = engine.connect()

query = text("SHOW DATABASES")
result = connection.execute(query)
for row in result:
    print(row[0])
