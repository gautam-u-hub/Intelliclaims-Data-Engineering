import os
import pymongo
import csv
import pandas as pd
from datetime import datetime

FILES_FOLDER = "files"
TIMESTAMP_FILE = os.path.join(FILES_FOLDER, "last_timestamp.txt")

def get_last_timestamp():
    if os.path.exists(TIMESTAMP_FILE):
        with open(TIMESTAMP_FILE, "r") as file:
            return file.read().strip()
    return None

def save_last_timestamp(timestamp):
    with open(TIMESTAMP_FILE, "w") as file:
        file.write(timestamp.isoformat())

def mongo_to_csv(collection, csv_file, last_updated_timestamp=None):
    client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
    db = client["claims"]
    col = db[collection]

    query = {}
    if last_updated_timestamp:
        query["updatedAt"] = {"$gt": last_updated_timestamp}
    
    data = list(col.find(query))
    if not data:
        print(f"No new data found in MongoDB collection '{collection}'.")
        return

    field_names = list(data[0].keys())

    for document in data:
        if 'updatedAt' in document:
            document['updatedAt'] = document['updatedAt'].strftime('%Y-%m-%d %H:%M:%S')

    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(data)

    latest_timestamp = datetime.now()
    save_last_timestamp(latest_timestamp)


    return field_names

def csv_to_parquet(csv_file, parquet_file):
    df = pd.read_csv(csv_file)
    df.to_parquet(parquet_file, index=False)

def export_collections_to_parquet():
    last_updated_timestamp = get_last_timestamp()

    client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
    db = client["claims"]
    collections = db.list_collection_names()

    for collection in collections:
        csv_file = os.path.join(FILES_FOLDER, f"{collection}.csv")
        parquet_file = os.path.join(FILES_FOLDER, f"{collection}.parquet")

        field_names = mongo_to_csv(collection, csv_file, last_updated_timestamp)
        if field_names:
            csv_to_parquet(csv_file, parquet_file)
            print(f"Parquet file created successfully for collection '{collection}': {parquet_file}")
        else:
            print(f"No new data found for collection '{collection}'.")

if not os.path.exists(FILES_FOLDER):
    os.makedirs(FILES_FOLDER)
