import os
import json
from pymongo import MongoClient, errors
from datetime import datetime


def connect_to_mongo(uri, db_name):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Check connection
        print(f"[{datetime.now()}] Connected to MongoDB successfully.")
        print("--------------------------------------------------------")
        return client[db_name]
    except errors.ServerSelectionTimeoutError as err:
        print(f"[{datetime.now()}] Could not connect to MongoDB: {err}")
        print("--------------------------------------------------------")
        return None


def import_json_files_to_mongo(db, data_folder, clear_old=True):
    if not os.path.exists(data_folder):
        print(f"[{datetime.now()}] ❌ Data folder not found: {data_folder}")
        return

    for file_name in os.listdir(data_folder):
        if file_name.endswith(".json"):
            collection_name = os.path.splitext(file_name)[0]
            collection = db[collection_name]
            file_path = os.path.join(data_folder, file_name)

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    data = json.load(file)

                # Clear existing data if required
                if clear_old:
                    deleted_count = collection.delete_many({}).deleted_count
                    print(f"[{datetime.now()}] 🗑 Cleared {deleted_count} records from '{collection_name}'.")

                if isinstance(data, list):
                    if data:
                        collection.insert_many(data)
                        print(f"[{datetime.now()}] ✅ Imported {len(data)} records into '{collection_name}' collection.")
                        print("")
                    else:
                        print(f"[{datetime.now()}] ⚠️ {file_name} is empty, skipped.")
                        print("")
                else:
                    collection.insert_one(data)
                    print(f"[{datetime.now()}] ✅ Imported 1 record into '{collection_name}' collection.")
                    print("")

            except json.JSONDecodeError as json_err:
                print(f"[{datetime.now()}] ❌ JSON format error in {file_name}: {json_err}")
                print("")
            except errors.BulkWriteError as bulk_err:
                print(f"[{datetime.now()}] ❌ Bulk write error for {file_name}: {bulk_err.details}")
                print("")
            except Exception as e:
                print(f"[{datetime.now()}] ❌ Unexpected error while importing {file_name}: {e}")
                print("")