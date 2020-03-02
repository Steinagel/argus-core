import json
from datetime import datetime
from analyser_config import collection, logger, _analyzer
from bson.objectid import ObjectId
import pickle


def analyse(msg):
    data = json.loads(msg.decode())

    mongo_id = data["mongo_id"]
    elasticsearch_id = data["elasticsearch_id"]

    logger.info(f"Started analyser {elasticsearch_id}")

    query    = {'_id':ObjectId(mongo_id)}

    doc             = collection.find_one(query)
    sentences       = doc["sentences"] 
    analyze_result  = _analyzer(sentences)

    update   = { "$set": { "processing": False , "analysis": analyze_result, "lastAttpemt": datetime.utcnow()} }
    
    collection.update_one(query, update)

    logger.info(f"Finished analyser {mongo_id}")
