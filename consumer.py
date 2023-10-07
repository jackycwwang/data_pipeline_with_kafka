import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer



# Define Kafka configuration
# TODO Change the following configurations
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'E3IAKOQ2TDCI3T7P',
    'sasl.password': 'C2ISLqGvNcJWcuaCZIs+UoypNWIhs4cFh9zyVmPS1X5VZ5R52A0oqRIASt2MVbtb',
    'group.id': 'group11',
    # 'auto.offset.reset': 'earliest'
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

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)


