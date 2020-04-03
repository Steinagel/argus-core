import tika
tika.initVM()
from tika import parser
from tika import language
import json
from bson import json_util
from bson.objectid import ObjectId
import os, requests, uuid
import zlib
import re
from textwrap import wrap
from datetime import datetime
from nltk import tokenize
from content_miner_config import PRODUCE_MESSAGE, logger, collection, subscription_key, endpoint, elasticsearch
if PRODUCE_MESSAGE:
     from content_miner_config import producer, topic, delivery_callback

def send(msg):
     try:
          # Load message
          data = json.loads(msg.decode())
          # Get mondo_id
          mongo_id = data["mongo_id"]
          logger.info(f"Started tika-parsing {mongo_id}")
          # Recover object infos from mongodb
          query       = {'_id':ObjectId(mongo_id)}
          try:
               doc    = collection.find(query)[0]
          except:
               update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
               collection.update_one(query, update)
               return "Failed to find object"
          source_code = doc["source_code"]

          # Get information form tika
          try:
               parsed = parser.from_buffer(source_code)
          except:
               update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
               collection.update_one(query, update)
               return "Failed to parse source code"

          if parsed["status"] is 404:
               return "Error 404 from Tika."

          metadata = parsed["metadata"]
          content  = parsed["content"]

          # Clear content
          try:
               content = _clear_text(content)
          except:
               update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
               collection.update_one(query, update)
               return "Failed to clear content"
          # Break into sentences

          content_lang = language.from_buffer(content)
          en_sentences = []
          # Translation
          try:
               breaked_sent = _break_text(content)
          except:
               update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
               collection.update_one(query, update)
               return "Failed to break into sentences"


          for sentence in breaked_sent:
               _obj = {}
               lang       = language.from_buffer(sentence)
               if lang != 'en':
                    _obj = {"translated": True, "sentence": _translate(subscription_key,
                                                                      endpoint,
                                                                      sentence,
                                                                      'en')}
               else:
                    _obj = {"translated": False, "sentence": sentence}
               en_sentences.append(_obj)

          logger.info(f"Finished tika-parsing {mongo_id}")
          # Sends to elasticsearch
          data             =    { "content": content,"mongo_id": mongo_id }
          
          elasticsearch_id = _send_to_elasticsearch(data)
          # Updates mongodb
          update = {
               "elasticsearch_id": elasticsearch_id, 
               "sentences": en_sentences, 
               "content": content, 
               "language": content_lang, 
               "metadata": metadata
          }
          collection.update_one(query, { "$set": update })

          # Updates message
          data.update({"elasticsearch_id": elasticsearch_id})

          # Sends to kafka - analysis
          try:
               if PRODUCE_MESSAGE:
                    producer.poll(0)
                    producer.produce(topic, json.dumps(data, default=json_util.default).encode(), callback=delivery_callback)
                    producer.flush()
          except:
               update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
               collection.update_one(query, update)
               return "Failed to delivery"
     except:
          update = { "$set": { "processing": False , "last_verify": datetime.utcnow()} }
          query = {'_id':ObjectId(mongo_id)}
          collection.update_one(query, update)
          return "Unable to mine"

def _send_to_elasticsearch(data):
     logger.info(f"Sending content to elasticsearch")

     res  = elasticsearch.index(index='arguscontent',doc_type='sourcecontent',body=data)
     _id  = res["_id"]

     logger.info(f"Sent to elasticsearch - [_id {_id}]")

     return _id

def _translate(subscription_key, endpoint, text, lang):
     logger.info(f"Translating...")
     path = '/translate?api-version=3.0'
     params = f'&to={lang}'
     constructed_url = endpoint + path + params

     headers = {
          'Ocp-Apim-Subscription-Key': subscription_key,
          'Content-type': 'application/json',
          'X-ClientTraceId': str(uuid.uuid4())
     }

     body = [{
          'text': text
     }]

     request = requests.post(constructed_url, headers=headers, json=body)
     response = request.json()
     logger.info(f"Translated")
     return response

def _clear_text(text):
     if text:
          text = re.sub('[.]+', '. ', text)
          text = re.sub('[\t\n]+', '. ', text)
          text = re.sub('[\n\t]+', '. ', text)
          text = re.sub('[\s ]+', ' ', text)
          text = re.sub('[\\"]+', '\'', text)
          text = re.sub('[\"]+', '\'', text)
          text = re.sub('(. )+', '. ', text)
          logger.info(f"Text cleared")
     else:
          logger.info(f"Text is None")

     return text

def _break_text(text):
     if text:
          sentences = tokenize.sent_tokenize(text)
          sub_text  = ""
          result = wrap(' '.join(sentences), 5000)

          logger.info(f"Text breaked in {len(result)} parts")
     else:
          logger.info(f"Text is None")
     return result
