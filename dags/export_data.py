import os
import pandas as pd
import pymongo
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

client = pymongo.MongoClient("mongodb://host.docker.internal:27017/")
db = client["claims"]

def mongo_to_parquet(collection_name, script_dir, last_timestamp):
    collection = db[collection_name]
    updated_data = list(collection.find({"updatedAt": {"$gt": last_timestamp}}))
    
    if updated_data:
        new_df = pd.DataFrame(updated_data)

        for column in new_df.select_dtypes(include=['object']).columns:
            new_df[column] = new_df[column].astype(str)

        files_dir = os.path.join(script_dir, "files")
        if not os.path.exists(files_dir):
            os.makedirs(files_dir)

        parquet_filename = os.path.join(files_dir, f"{collection_name}.parquet")

        if os.path.exists(parquet_filename):
            existing_table = pq.read_table(parquet_filename)
            existing_df = existing_table.to_pandas()
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            merged_df.drop_duplicates(subset='_id', keep='last', inplace=True)
            table = pa.Table.from_pandas(merged_df)
            pq.write_table(table, parquet_filename)
        else:
            table = pa.Table.from_pandas(new_df)
            pq.write_table(table, parquet_filename)
        
        print(f"Data from collection '{collection_name}' converted to Parquet successfully.")
    else:
        print(f"No new or updated data found in collection '{collection_name}'.")

def mongo_to_csv(collection_name, script_dir, last_timestamp):
    collection = db[collection_name]
    updated_data = list(collection.find({"updatedAt": {"$gt": last_timestamp}}))
    
    if updated_data:
        new_df = pd.DataFrame(updated_data)

        for column in new_df.select_dtypes(include=['object']).columns:
            new_df[column] = new_df[column].astype(str)

        files_dir = os.path.join(script_dir, "files")
        if not os.path.exists(files_dir):
            os.makedirs(files_dir)

        csv_filename = os.path.join(files_dir, f"{collection_name}.csv")

        if os.path.exists(csv_filename):
            existing_df = pd.read_csv(csv_filename)
            for i, row in new_df.iterrows():
                existing_df = existing_df[existing_df['_id'] != row['_id']]
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            merged_df.to_csv(csv_filename, index=False)
        else:
            new_df.to_csv(csv_filename, index=False)

        print(f"Data from collection '{collection_name}' appended to CSV successfully.")
    else:
        print(f"No new or updated data found in collection '{collection_name}'.")

def get_last_timestamp(script_dir):
    try:
        with open(os.path.join(script_dir, "files", "last_run_timestamp.txt"), "r") as file:
            return datetime.fromisoformat(file.read().strip())
    except FileNotFoundError:
        return datetime.min

def update_last_timestamp(timestamp, script_dir):
    timestamp_path = os.path.join(script_dir, "files", "last_run_timestamp.txt")
    
    with open(timestamp_path, "w") as file:
        file.write(timestamp.isoformat())

def fun():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    last_timestamp = get_last_timestamp(script_dir)
    
    for collection_name in db.list_collection_names():
        mongo_to_csv(collection_name, script_dir, last_timestamp)
        mongo_to_parquet(collection_name, script_dir, last_timestamp)
    
    update_last_timestamp(datetime.now(), script_dir)
