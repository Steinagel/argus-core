from flask import Flask, request, jsonify
from flask_cors import CORS
from controller_config import (elsearch, 
                                elasticsearch, 
                                INDEX, 
                                validateJson, 
                                add_new_url, 
                                disable_url_by_adress,
                                enable_url_by_adress,
                                get_all_urls,
                                get_url)

app = Flask(__name__)
CORS(app)

@app.route('/search', methods=["POST"])
def search():
    data     = request.json

    schema = {
        "type" : "object",
        "content" : {"type" : "string"}
    }

    if not validateJson(data, schema):
        return {"message": "Error, expecting structure: {\"content\": \"<string>\"}"}

    is_there = elsearch(data, INDEX)

    return {"was_found": is_there}

@app.route('/add_url', methods=["POST"])
def add_url():
    data      = request.json

    schema = {
        "type" : "object",
        "url" : {"type" : "string"}
    }

    if not validateJson(data, schema):
        return {"message": "Error, expecting structure: {\"url\": \"<string>\"}"}

    if add_new_url(data["url"]):
        return {"Status": "success"}
    else:
        return {"Status": "fail"}

@app.route('/disable_url', methods=["PUT"])
def disable_url():
    data      = request.json

    schema = {
        "type" : "object",
        "url" : {"type" : "string"}
    }

    if not validateJson(data, schema):
        return {"message": "Error, expecting structure: {\"url\": \"<string>\"}"}

    if disable_url_by_adress(data["url"]):
        return {"Status": "success"}
    else:
        return {"Status": "fail"}

@app.route('/enable_url', methods=["PUT"])
def enable_url():
    data      = request.json

    schema = {
        "type" : "object",
        "url" : {"type" : "string"}
    }

    if not validateJson(data, schema):
        return {"message": "Error, expecting structure: {\"url\": \"<string>\"}"}

    if enable_url_by_adress(data["url"]):
        return {"Status": "success"}
    else:
        return {"Status": "fail"}

@app.route('/urls', methods=["GET"])
def get_urls():
    return get_all_urls()

@app.route('/url/<id>', methods=["GET"])
def get_a_url(id):
    return get_url(id)

if __name__ == '__main__':
    app.run()
