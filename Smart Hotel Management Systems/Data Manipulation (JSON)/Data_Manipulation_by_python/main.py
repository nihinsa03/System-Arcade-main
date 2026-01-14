from datetime import datetime
from Data_Manipulation_by_python.manipulation import connect_to_mongo, import_json_files_to_mongo


def main():
    uri = "mongodb://localhost:27017/"
    db_name = "RestaurantManagementSystem"
    data_folder = r"D:\System Arcade\Smart Hotel Management Systems\Data Manipulation (JSON)\Data_Structures"

    db = connect_to_mongo(uri, db_name)
    if db is not None:
        import_json_files_to_mongo(db, data_folder, clear_old=True)
        print(f"[{datetime.now()}] 🎉 All JSON files processed!")
    else:
        print(f"[{datetime.now()}] ❌ Database connection failed. Exiting.")



if __name__ == "__main__":
    main()