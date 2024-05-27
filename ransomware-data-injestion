import json
import requests
from elasticsearch import Elasticsearch, exceptions
import hashlib

## elastic search properties
client = Elasticsearch(
    "https://b01b2ccfad08422f8985bbece2ec412c.asia-south1.gcp.elastic-cloud.com:443",
    api_key="Snh4eXVJOEJ3cEVNZTUxSTFuVms6QkM1dk5kVGZSUi1vU0JZNE1xalJfUQ=="
)
index_name = 'ransomware'

## source
url = "https://raw.githubusercontent.com/codingo/Ransomware-Json-Dataset/master/ransomware_overview.json"
failedRecords = []

## fetch Data from source
def fetchData():
    response = requests.get(url)
    data = response.json()
    return data

## generate unique id for the doc
def generate_unique_id(doc):
    doc_str = json.dumps(doc, sort_keys=True)
    return hashlib.md5(doc_str.encode('utf-8')).hexdigest()

# process the data to DB
def processData(data):
    for doc in data:
        print(doc)
        res = updateToDB(doc)

# interface to connect db
def updateToDB(doc):
    unique_id = generate_unique_id(doc)
    try:
        if client.exists(index=index_name, id=unique_id):
            failedRecords.append(doc)
            print(f"Document is already exists")
            return
            
        response = client.index(index=index_name, id=unique_id, body=doc)
        print(f"Document indexed successfully: {response}")
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        failedRecords.append(doc)

## calling methods
jsonData = fetchData()

# process and save the doc to database
processData(jsonData)

# if any failed resords
if failedRecords:
    with open('failed_records.json', 'w') as f:
        json.dump(failedRecords, f)
