from confluent_kafka import Consumer, KafkaException
from confluent_kafka import Producer
import logging
import pymongo
import json
import sys
import os
from pprint import pformat
from elasticsearch import Elasticsearch

### Microsoft Translator
ms_key = 'TRANSLATOR_TEXT_SUBSCRIPTION_KEY'
if not ms_key in os.environ:
    raise Exception('Please set/export the environment variable: {}'.format(ms_key))
subscription_key = os.environ[ms_key]

# https://curiosityy.cognitiveservices.azure.com/translator/text/v3.0
ms_endpoint = 'TRANSLATOR_TEXT_ENDPOINT'
if not ms_endpoint in os.environ:
    raise Exception('Please set/export the environment variable: {}'.format(ms_endpoint))
endpoint = os.environ[ms_endpoint]
##

### Logger
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)
##

### MongoDB
try:  
     db_user = os.environ["MONGO_INITDB_USERNAME"]
     db_password = os.environ["MONGO_INITDB_PASSWORD"]
     db_database = os.environ["MONGO_INITDB_DATABASE"]
     db_collection = os.environ["INITDB_COLLECTION"]
except KeyError: 
     raise KeyError("Please set MONGO_INITDB_USERNAME, MONGO_INITDB_DATABASE, MONGO_INITDB_PASSWORD and INITDB_COLLECTION environment variables!")

MONGO_CLIENT = pymongo.MongoClient(f"mongodb://{db_user}:{db_password}@mongodb:27017/{db_database}")

db = MONGO_CLIENT[db_database]
collection = db[db_collection]
##

### Kafka
## Consumer
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    logger.info('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))

broker = 'kafka1:9092'
group = 'argusgroup'
topics = ['tika']

conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'stats_cb': stats_cb, 'fetch.message.max.bytes': 1000000000,
            'fetch.max.bytes': 2147483135, 'message.max.bytes': 1000000000, 'max.poll.interval.ms': 600000} #, 'enable.auto.commit': False

CONSUMER = Consumer(conf, logger=logger)
#

# Producer
def print_assignment(consumer, partitions):
    logger.info(f'Assignment: {partitions}')

CONSUMER.subscribe(topics, on_assign=print_assignment)

PRODUCE_MESSAGE = True

if PRODUCE_MESSAGE:
     #TODO Change to 9092 when runing on containers
     producer = Producer({'bootstrap.servers': broker})

     topic = 'analysis'

     def delivery_callback(err, msg):
          if err:
               sys.stderr.write('%% Message failed delivery: %s\n' % err)
          else:
               sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))
##

### Elasticsearch
elasticsearch = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
#