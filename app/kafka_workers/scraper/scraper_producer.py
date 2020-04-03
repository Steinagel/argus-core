import json
import hashlib
import importlib
from datetime import datetime
from scrap_collect.collect import Scrap
from scraper_config import PRODUCE_MESSAGE, logger, collection
from bson.objectid import ObjectId
from bson import json_util
import sys
import zlib
if PRODUCE_MESSAGE:
     from scraper_config import producer, topic, delivery_callback


def scrap(msg):
    # Load message
    data = json.loads(msg.decode('utf8'))

    # Get mondo infos
    mongo_id = data["mongo_id"]
    url      = data["url"]
    md5      = data["md5"]

    logger.info(f"Started scraper {url}...")

    # Scrap
    try:
        tab_scrap = Scrap(url)
    except:
        update    = { "$set": { "processing": False , "lastAttpemt": datetime.utcnow()} }
        a_query   = {'_id':ObjectId(mongo_id)}

        collection.update_one(a_query, update)

        return "Unable to scrap. Updating mongo for that."

    if tab_scrap.get_source_code() is None:
        update  = { "$set": { "processing": False , "enabled": False, "lastAttpemt": datetime.utcnow()} }
        a_query = {'_id':ObjectId(mongo_id)}

        collection.update_one(a_query, update)

        logger.info(f"Error requesting {url}. Disabling it.")

        return "Fail. Invalid URL disabled."
        
    tab_source_code = tab_scrap.get_source_code().decode('utf-8')
    tab_md5         = hashlib.md5(
                        tab_source_code.encode()).hexdigest()


    # Verify md5
    if not md5 or tab_md5 != md5: 
        tab_links       = tab_scrap.get_links()

        update = { "$set": 
                    { "source_code": tab_source_code, 
                      "links": tab_links, 
                      "md5": tab_md5, 
                      "lastAttpemt": datetime.utcnow(),
                      "lastChange": datetime.utcnow()
                    } 
                }
        query  = {'_id':ObjectId(mongo_id)}

        collection.update_one(query, update)

        logger.info(f"Finished scraper {url[:15]}... [contains new data]")
        try:
            if PRODUCE_MESSAGE:
                producer.poll(0)
                producer.produce(topic, msg, callback=delivery_callback)
                producer.flush()
        except:
            update = { "$set": { "processing": False , "lastAttpemt": datetime.utcnow()} }
            collection.update_one(query, update)
            return f"Failed to delivery message to {topic}"
    else:
        query    = {'_id':ObjectId(mongo_id)}
        update   = { "$set": { "processing": False, "lastAttpemt": datetime.utcnow()} }
        collection.update_one(query, update)
        logger.info(f"Finished scraper {url[:15]}... [without changes]")

    return "Done. Success."