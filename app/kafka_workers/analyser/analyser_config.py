from confluent_kafka import Consumer, KafkaException
from confluent_kafka import Producer
import logging
import pymongo
import json
import sys
import os
from pprint import pformat
import pickle
from sklearn.feature_extraction.text import CountVectorizer
from collections import Counter
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

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

MONGO_CLIENT = pymongo.MongoClient(f"mongodb://{db_user}:{db_password}@mongodb:27017/{db_database}")

db = MONGO_CLIENT[db_database]
collection= db[db_collection]
##

### Kafka
## Consumer
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    logger.info('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))

broker = 'kafka1:9092'
group = 'argusgroup'
topics = ['analysis']

conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'stats_cb': stats_cb, 'fetch.message.max.bytes': 1000000000,
            'fetch.max.bytes': 2147483135, 'message.max.bytes': 1000000000, 'max.poll.interval.ms': 600000} #, 'enable.auto.commit': False

CONSUMER = Consumer(conf, logger=logger)

def print_assignment(consumer, partitions):
    logger.info(f'Assignment: {partitions}')

CONSUMER.subscribe(topics, on_assign=print_assignment)
##

### Analizer
model = '/usr/src/app/app/kafka_workers/analyser/model.sav'
loaded_model = pickle.load(open(model, 'rb'))

vector = '/usr/src/app/app/kafka_workers/analyser/vectorizer.sav'
count_vector = pickle.load(open(vector, 'rb'))


def has_rw(string):
    risky_words = ['media', 'information', 'breach', 'data', 'secret','secrets', 'confidential' 'top-secret', 'financial', 'account', 'accounts', 'password']

    for word in string.split():
        if word.lower() in risky_words:
            return True
    return False


def classify_risk(score):
    if score >= .75:
        return 'high'
    elif score >= .50:
        return 'medium'
    else:
        return 'low'


def predict_page(string):
    page_sentences = string.split('.')
    
    probabilities = [{'text': page_sentences[proba[0]],
                      'probability': proba[1][1],
                      'has_rw': has_rw(page_sentences[proba[0]]),
                      'risk_level': classify_risk(proba[1][1]*(2 if has_rw(page_sentences[proba[0]]) else 0.5))} 
                    for proba in enumerate(loaded_model.predict_proba(count_vector.transform(page_sentences)))]

    return probabilities

def _analyzer(sentences):
    total_content = ""
    _languages = []
    for _sentence in sentences:
        if _sentence["translated"]:
            for wrap_sentence in _sentence["sentence"]:
                _languages.append(wrap_sentence)
                for sentence in wrap_sentence["translations"]:
                    total_content+=sentence["text"]
        else:
            total_content+=_sentence["sentence"]

    return predict_page(total_content)

    # return " "