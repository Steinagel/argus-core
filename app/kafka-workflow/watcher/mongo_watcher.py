from watcher_config import collection, PRODUCE_MESSAGE
from bson.objectid import ObjectId
from bson import json_util
import json
if PRODUCE_MESSAGE:
     from watcher_config import producer, topic, delivery_callback
from base64 import b16encode

def watch():
     urls = []
     query = {'enabled': True, "processing": False, "queued": False}
     cursor = collection.find(query)
     
     for document in cursor:
          urls.append({"mongo_id": str(document['_id']),"url": document['url'], "md5": document['md5']})

     update = { "$set": { "queued": True } }
     collection.update_many(query, update)

     doc_urls       = []
     repeated_urls  = []
     
     for url in urls:
          if doc_urls == []:
               doc_urls.append(url)
          else:
               for doc_url in doc_urls:
                    # print(doc_url)
                    if url["mongo_id"] != doc_url["mongo_id"] and url["url"]==doc_url["url"]:
                         repeated_urls.append(url)
                    elif url not in repeated_urls and url not in doc_urls:
                         doc_urls.append(url)

     print(doc_urls)
     if repeated_urls:
          for r_url in repeated_urls:
               update = { "$set": { "enabled": False } }
               mongo_id    = r_url["mongo_id"]
               query = {'_id':ObjectId(mongo_id)}
               collection.update_one(query, update)

     if doc_urls:
          for url in doc_urls:
               if PRODUCE_MESSAGE:
                    producer.poll(0)
                    producer.produce(topic, json.dumps(url, default=json_util.default).encode('utf-8'), callback=delivery_callback)
               print(url)
     if PRODUCE_MESSAGE:
          producer.flush()

if __name__ == '__main__':
     while True:
          watch()
