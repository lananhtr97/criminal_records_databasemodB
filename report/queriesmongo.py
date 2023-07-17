from pymongo import MongoClient
from time import time
import csv

# MongoDB connection details
mongo_url = "mongodb://localhost:27017"
mongo_db_name = "databasemodb"

# Create a MongoDB client
client = MongoClient(mongo_url)

# Access the database
db = client[mongo_db_name]

# 1. All calls with their associated cell site information


def query_1():
    before = time()
    results = list(db["calls"].find({"id_cellsite": "18"}))
    after = time()
    return after - before


print(query_1())

# 2. Call for a specific user with their associated cell site information


def query_2():
    before = time()
    results = list(db["calls"].find(
        {"id_caller": "351(905)372-6077", "id_cellsite": "18"}))
    after = time()
    return after - before


print(query_2())

# 3. Calls for a specific user within a specific time range


def query_3():
    before = time()
    results = list(db["calls"].find({"id_caller": "351(905)372-6077", "duration": {"$gte": 4000}}))
    after = time()
    return after - before


print(query_3())

# 4. Known people from a phone number


def query_4():
    before = time()
    call_number = "504(630)761-4257"
    distinct_phone_numbers = db["calls"].distinct("id_receiver", {"id_caller": call_number})
    results = list(db["people"].find({"phone_number": {"$in": distinct_phone_numbers}}))
    after = time()
    return after - before


print(query_4())


def results_to_csv(results):
    with open('query_results_mongo.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Query', 'Execution Time'])

        for query, execution_times in results.items():
            for time in execution_times:
                writer.writerow([query, time])


def mongodb_query():
    query1results = []
    query2results = []
    query3results = []
    query4results = []
    queryExecutionTime = {"query1": [],
                          "query2": [], "query3": [], "query4": []}

    for i in range(31):
        query1results.append(query_1())
    for i in range(31):
        query2results.append(query_2())
    for i in range(31):
        query3results.append(query_3())
    for i in range(31):
        query4results.append(query_4())

    queryExecutionTime["query1"] = query1results
    queryExecutionTime["query2"] = query2results
    queryExecutionTime["query3"] = query3results
    queryExecutionTime["query4"] = query4results
    return queryExecutionTime


if __name__ == '__main__':
    results_mongo = mongodb_query()
    results_to_csv(results_mongo)
