import pymongo
import os
from confluent_kafka import Producer
import sys
import logging

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
##

### Kafka
# Producer
MONGO_CLIENT = pymongo.MongoClient(f"mongodb://{db_user}:{db_password}@mongodb:27017/{db_database}")

db = MONGO_CLIENT[db_database]
collection = db[db_collection]

PRODUCE_MESSAGE = True

if PRODUCE_MESSAGE:
     #TODO Change to 9092 when runing on containers
     producer = Producer({'bootstrap.servers': 'kafka1:9092'})

     topic = 'scraper'

     def delivery_callback(err, msg):
          if err:
               sys.stderr.write('%% Message failed delivery: %s\n' % err)
          else:
               sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))
##