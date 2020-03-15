from confluent_kafka import Consumer, KafkaException
from confluent_kafka import Producer
import logging
import pymongo
import json
import sys
import os
from pprint import pformat

logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

try:  
     db_user = os.environ["MONGO_INITDB_USERNAME"]
     db_password = os.environ["MONGO_INITDB_PASSWORD"]
     db_database = os.environ["MONGO_INITDB_DATABASE"]
     db_collection = os.environ["INITDB_COLLECTION"]
except KeyError: 
     raise KeyError("Please set INITDB_DATABASE_USER, INITDB_DATABASE_PASSWORD and INITDB_COLLECTION environment variables!")

MONGO_CLIENT = pymongo.MongoClient(f"mongodb://{db_user}:{db_password}@localhost:27017/{db_database}")

db = MONGO_CLIENT[db_database]
collection= db[db_collection]

def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))

broker = 'localhost:29092'
group = 'argusgroup'
topics = ['scraper']

conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'stats_cb': stats_cb, 'fetch.message.max.bytes': 1000000000,
            'fetch.max.bytes': 2147483135, 'message.max.bytes': 1000000000}

CONSUMER = Consumer(conf)

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

CONSUMER.subscribe(topics, on_assign=print_assignment)

PRODUCE_MESSAGE = True

if PRODUCE_MESSAGE:
     #TODO Change to 9092 when runing on containers
     producer = Producer({'bootstrap.servers': 'localhost:29092'})

     topic = 'tika'

     def delivery_callback(err, msg):
          if err:
               sys.stderr.write('%% Message failed delivery: %s\n' % err)
          else:
               sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))