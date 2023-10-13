import os
import logging
import json
import threading

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import hydra
from omegaconf import OmegaConf, DictConfig

consumer_name = os.getenv('CONSUMER_NAME')

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("consumer_logger")

@hydra.main(version_base=None, config_path='conf', config_name='config')
def consumer_app(cfg: DictConfig):
    sr_cfg = cfg['schema_registry']
    kafka_cfg = cfg['kafka']

    # ---------- Create a Schema Registry client ----------
    schema_registry_client = SchemaRegistryClient({
      'url': sr_cfg['url'],
      'basic.auth.user.info': '{}:{}'.format(sr_cfg['user'], sr_cfg['secret'])
    })

    # Fetch the latest Avro schema for the value
    subject_name = 'product_updates-value'
    schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
    # print('schema_str:',schema_str)

    # ---------- Create a Kafka consumer -------------
    key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Define Kafka configuration
    kafka_config = {
      'bootstrap.servers': kafka_cfg['bootstrap_servers'],
      'sasl.mechanisms': kafka_cfg['sasl_mechanisms'],
      'security.protocol': kafka_cfg['security_protocol'],
      'sasl.username': kafka_cfg['sasl_username'],
      'sasl.password': kafka_cfg['sasl_password'],
      'key.deserializer': key_deserializer,
      'value.deserializer': avro_deserializer,
      'group.id': 'cgrp1',
      'auto.offset.reset': 'earliest',
      # 'enable.auto.commit': True,      # default value
      # 'auto.commit.interval.ms': 5000, # Commit every 5000 ms, i.e., every 5 seconds
      # 'max.poll.interval.ms': 300000,
    }

    # Define the DeserializingConsumer
    try:
      consumer = DeserializingConsumer(kafka_config)
    except Exception as e:
      logger.error(f"Kafka connection failed: {e}")

    # Subscribe to the 'product_updates' topic
    consumer.subscribe(['product_updates'])

    # --------- Load the updates log ----------
    log_path = './consumer_log/consumer_log.json'
    if not os.path.exists(log_path):
      updates = {'update_logs': []}
    else:
      try:
        with open(log_path, 'r') as log_fp:
          updates = json.load(log_fp)
      except FileNotFoundError as file_err:
        logger.error(f"File not found: {file_err}")
        updates = {'update_logs': []}
      except json.JSONDecodeError as json_err:
        logger.error(f"JSON Read error: {json_err}")
        updates = {'update_logs': []}
      except Exception as err:
        logger.error(f"An error occurred: {err}")
        updates = {'update_logs': []}

    # ------------ Start consuming the message ------------
    discount = 0.8
    try:
      while True:
        msg = consumer.poll(1.0)
        if msg is None:
          continue
        if msg.error():
          print('Consumer error: {}'.format(msg.error()))
          continue

        value = msg.value()
        category = value['category']
        value['category'] = category.upper()
        value['discount_price'] = 'na'
        if category == 'Cat_5':
          value['discount_price'] = round(value['price'] * discount, 2)

        try:
          updates['update_logs'].append(value)
          with open(log_path, 'w') as log_fp:
            json.dump(updates, log_fp, indent=2)
        except FileNotFoundError as file_err:
          print(f"Error: {log_path} not found: {file_err}")
        except json.JSONDecodeError as json_err:
          print(f"JSON Write error: {json_err}")
        except Exception as err:
          print(f"An error occurred: {err}")

        logger.info('Id {} is successfully consumed by {}.'.format(value['id'], consumer_name))

        # time.sleep(30)
        #consumer.commitSync()
        #consumer.commitAsync()
    except Exception as e:
      logger.error(f"Un unexpected error occurred: {e}")

    finally:
      consumer.close()


if __name__ == '__main__':
    consumer_app()


