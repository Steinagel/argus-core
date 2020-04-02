from elasticsearch import Elasticsearch
import json
import os
from jsonschema import validate
from pymongo import MongoClient 
from bson import json_util, ObjectId



### MongoDB
try:  
     db_user = os.environ["MONGO_INITDB_USERNAME"]
     db_password = os.environ["MONGO_INITDB_PASSWORD"]
     db_database = os.environ["MONGO_INITDB_DATABASE"]
     db_collection = os.environ["INITDB_COLLECTION"]
except KeyError: 
     raise KeyError("Please set MONGO_INITDB_USERNAME, MONGO_INITDB_DATABASE, MONGO_INITDB_PASSWORD and INITDB_COLLECTION environment variables!")

MONGO_CLIENT = MongoClient(f"mongodb://{db_user}:{db_password}@mongodb:27017/{db_database}")

db = MONGO_CLIENT[db_database]
collection = db[db_collection]

def get_all_urls():
    cursor = collection.find({}, { "_id": 0 })
    list_elements = [json.loads(json_util.dumps(el)) for el in cursor]

    return {"result": list_elements}

def get_url(id):
    try:
        element = collection.find_one({'_id':ObjectId(id)})
    except:
        return None

    return json.loads(json_util.dumps(element))
##

### Elasticsearch
INDEX='arguscontent'

elasticsearch = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

def elsearch(key_value, index=INDEX):
    res = elasticsearch.search(index=index,body={'query':{'bool':{'must':[{'match':key_value}]}}})
    return bool(res['hits']['hits'])
#


def validateJson(data, schema):
    try:
        validate(instance=data, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True

def add_new_url(url):
    try:
        _data = {
            "url": url,
            "md5": None,
            "enabled": True,
            "processing": False,
            "analysis": "",
            "source_code": "",
            "links": None,
            "last_verify": None,
            "elasticsearch_id": None,
            "interval": 2
        }
        collection.insert_one(_data)
    except:
        return False
    return True

def disable_url_by_adress(url):
    try:
        query  = {'url':url}
        update = { "$set": { "processing": False , "enabled": False} }
        collection.update_many(query, update)
    except:
        return False
    return True

def enable_url_by_adress(url):
    try:
        query  = {'url':url, "enabled": False}
        update = { "$set": { "processing": False , "enabled": True} }
        collection.update_one(query, update)
    except:
        return False
    return True