import pymongo
import os
from confluent_kafka import Producer
import sys

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

PRODUCE_MESSAGE = True

if PRODUCE_MESSAGE:
     #TODO Change to 9092 when runing on containers
     producer = Producer({'bootstrap.servers': 'localhost:29092'})

     topic = 'scraper'

     def delivery_callback(err, msg):
          if err:
               sys.stderr.write('%% Message failed delivery: %s\n' % err)
          else:
               sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))