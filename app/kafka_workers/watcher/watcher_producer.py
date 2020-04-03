from watcher_config import collection, PRODUCE_MESSAGE, logger
from bson.objectid import ObjectId
from bson import json_util
import json
from time import sleep
from datetime import datetime, timedelta
if PRODUCE_MESSAGE:
     from watcher_config import producer, topic, delivery_callback
from base64 import b16encode

def watcher_produce():
     # Get elements on mongodb
     query = {'enabled': True, 'processing': False } # TODO: optimize: 'lastAttpemt': {'$set': ()}
     cursor = collection.find(query)
     
     # Appends found urls if the interval has passed
     urls = []
     for document in cursor:
          if 'lastAttpemt' not in document.keys() or\
             document['lastAttpemt'] is None or \
             document['lastAttpemt']  < datetime.utcnow() - timedelta(minutes=document["interval"]):
               logger.info(f"found {document['url']}")
               urls.append({"mongo_id": str(document['_id']),"url": document['url'], "md5": document['md5']})

     # Find repeated urls
     doc_urls       = []
     repeated_urls  = []
     for url in urls:
          if doc_urls == []:
               doc_urls.append(url)
          else:
               for doc_url in doc_urls:
                    if url["mongo_id"] != doc_url["mongo_id"] and url["url"]==doc_url["url"]:
                         repeated_urls.append(url)
                    elif url not in repeated_urls and url not in doc_urls:
                         doc_urls.append(url)

     # Disable repeated urls on mongodb
     if repeated_urls:
          for r_url in repeated_urls:
               logger.info(f"Disabling {r_url['mongo_id']} from mongo")
               update = { "$set": { "enabled": False } }
               mongo_id    = r_url["mongo_id"]
               query = {'_id':ObjectId(mongo_id)}
               collection.update_one(query, update)

     # Sends to kafka - scraper
     if doc_urls:
          for url in doc_urls:
               if PRODUCE_MESSAGE:
                    try:
                         producer.poll(0)
                         producer.produce(topic, json.dumps(url, default=json_util.default).encode(), callback=delivery_callback)
                         mongo_id    = url["mongo_id"]
                         query = {'_id':ObjectId(mongo_id)}
                         update = { "$set": { "processing": True} }
                         collection.update_many(query, update)
                    except:
                         update = { "$set": { "processing": False , "lastAttpemt": datetime.utcnow()} }
                         e_query = {'_id':ObjectId(mongo_id)}
                         collection.update_one(e_query, update)
                         return "Failed to delivery."

     if PRODUCE_MESSAGE:
          producer.flush()

if __name__=='__main__':
     logger.info("Init watcher producer loop")
     while True:
          watcher_produce()