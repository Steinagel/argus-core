#!/usr/bin/env python
# import tika
# tika.initVM()
# from tika import parser
# parsed = parser.from_file('testfiles/test.html')
# print(parsed["metadata"])
# print(parsed["content"])
import requests
from app.scrap_collect.collect import Scrap
from time import sleep

def run():
    while True:
        print("sla")
        sleep(2)

# panda_url = "http://www.africau.edu/images/default/sample.pdf"
# print(requests.get(panda_url).headers)
# wiki_panda = Scrap(panda_url)
# source_code = wiki_panda.get_source_code()
# print(source_code)
# links = wiki_panda.get_links()
# print(links)
# print(len(links))


#   {
#       id: <ID>, 
#       mongo_id,
#       url: <String>,
#       hashMD5: <hash>,
#       content: <String>,
#       analysis_result: {
#                            ...
#                         }
#   }

# operate queuing
#   - scrap source_code and urls if true
#   - populate metadata:
#       - compare hashMD5 of source code and : send to Elasticsearch NEW DATA if changes or is null, break else
#       - unique urls vs url list
#       - update url list
#   NEW DATA: 
#       - url still enabled?
#       - still scrap links?
#       - new hash MD5?
#       - source_code if new MD5
#       - content if new MD5
#       - new analysis if new MD5