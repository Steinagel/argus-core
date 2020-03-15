import json
import hashlib
import importlib
Scrap = importlib.import_module("scrap_collect")
from scraper_config import PRODUCE_MESSAGE, collection
from bson.objectid import ObjectId
from bson import json_util
import sys
import zlib
if PRODUCE_MESSAGE:
     from scraper_config import producer, topic, delivery_callback


def scrap(msg):
    data = json.loads(msg.decode("utf-8"))

    mongo_id    = data["mongo_id"]

    query = {'_id':ObjectId(mongo_id)}
    cursor = collection.find(query)

    update = { "$set": { "processing": True } }
    collection.update_one(query, update)
    collection.update_one(query, update, upsert=False)

    url         = data["url"]
    md5         = data["md5"]

    tab = Scrap(url)

    tab_source_code = tab.get_source_code()
    tab_links       = tab.get_links()
    tab_md5         = hashlib.md5(tab_source_code).hexdigest()

    data.update({"source_code": tab_source_code, "links": tab_links})

    if PRODUCE_MESSAGE:
        if not md5 or tab_md5 != md5:
            producer.poll(0)
            producer.produce(topic, zlib.compress(str(data).encode(),level=9), callback=delivery_callback)
            producer.flush()
