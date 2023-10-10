import logging
import json
import threading

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import hydra
from omegaconf import OmegaConf, DictConfig


logger = logging.getLogger("consumer_logger")

@hydra.main(version_base=None, config_path='conf', config_name='config')
def consumer_app(cfg: DictConfig):
    sr_cfg = cfg['schema_registry']
    kafka_cfg = cfg['kafka']


    # Create a Schema Registry client
    schema_registry_client = SchemaRegistryClient({
      'url': sr_cfg['url'],
      'basic.auth.user.info': '{}:{}'.format(sr_cfg['user'], sr_cfg['secret'])
    })

    # Fetch the latest Avro schema for the value
    subject_name = 'product_updates-value'
    schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
    print('schema_str:',schema_str)

    # Create Avro Deserializer for the value
    key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Define Kafka configuration
    kafka_config = {
        'bootstrap.servers': kafka_cfg['bootstrap_servers'],
        'sasl.mechanisms': kafka_cfg['sasl_mechanisms'],
        'security.protocol': kafka_cfg['security_protocol'],
        'sasl.username': kafka_cfg['sasl_username'],
        'sasl.password': kafka_cfg['sasl_password'],
        'group.id': 'consumer_group',
        # 'auto.offset.reset': 'earliest'
    }

    # Define the DeserializingConsumer
    consumer = DeserializingConsumer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': kafka_config['group.id'],
        # 'auto.offset.reset': kafka_config['auto.offset.reset'],
        # 'enable.auto.commit': True,
        # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
    })

    # Subscribe to the 'retail_data' topic
    consumer.subscribe(['product_updates'])

    # Continually read messages from Kafka
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
            #consumer.commitSync()
            #consumer.commitAsync()
    except Exception as e:
      logger.error(f"Un unexpected error occurred: {e}")

    finally:
      consumer.close()




if __name__ == '__main__':
    consumer_app()


