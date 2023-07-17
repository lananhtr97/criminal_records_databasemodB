from pymongo import MongoClient
import csv

# MongoDB connection details
mongo_url = "mongodb://localhost:27017"
mongo_db_name = "databasemodb"

# Create a MongoDB client
client = MongoClient(mongo_url)

# Access the database
db = client[mongo_db_name]

# Load data from CSV file and insert into MongoDB collection
with open("call_record.csv", "r") as file:
    csv_reader = csv.DictReader(file)
    calls_data = []
    for row in csv_reader:
        call_record = {
            "id_caller": row["CALLING_NUMBER"],
            "id_receiver": row["CALLED_NUMBER"],
            "start_date": int(row["START_DATE"]),
            "duration": int(row["DURATION"]),
            "end_date": int(row["END_DATE"]),
            "id_cellsite": (row["CELL_SITE"])
        }
        calls_data.append(call_record)

# Insert call records data into the Calls collection
    calls_collection = db["calls"]
    calls_collection.insert_many(calls_data)

# Load data from cellsite_data.csv and insert into Cellsites collection
with open("cellsite_data.csv", "r") as file:
    csv_reader = csv.DictReader(file)
    cellsites_data = []

    for row in csv_reader:
        cellsites = {
            "cellsite": row["cell_site"],
            "city": row["city"],
            "state": row["state"],
            "address": row["address"]
        }
        cellsites_data.append(cellsites)

# Insert cellsites data into the Cellsites collection
    cellsites_collection = db["cell_sites"]
    cellsites_collection.insert_many(cellsites_data)

# Load data from people_data.csv and insert into People collection
with open("people_data.csv", "r") as file:
    csv_reader = csv.DictReader(file)
    people_data = []
    for row in csv_reader:
        person = {
            "full_name": row["full_name"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "phone_number": row["phone_number"]
        }
        people_data.append(person)

# Insert people data into the People collection
    people_collection = db["people"]
    people_collection.insert_many(people_data)

