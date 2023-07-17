from neo4j import GraphDatabase  # neo4j la thu vien
import os
from time import time
import csv

url = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
neo4jVersion = os.getenv("NEO4J_VERSION", "5")
database = ("Database Mod B", "neo4j")

driver = GraphDatabase.driver(url, auth=(username, password))
sessionDB = driver.session()

# 1 all calls with their associated cell site information (tat ca call cua cellsite 18)


def query_1(session):
    before = time()
    results = list(session.run(
        "MATCH (c:CALL)-[r:LOCATED_IN]->(l:LOCATION {cell_site: '18'}) RETURN DISTINCT c"))
    after = time()
    return after - before


print(query_1(sessionDB))

# 2 call for a specific user with their associated cell site information


def query_2(session):
    before = time()
    results = list(session.run(
        "MATCH (p:PERSON {number:'351(905)372-6077'})-[:MADE_CALL]->(c:CALL)-[:LOCATED_IN]->(l:LOCATION {cell_site: '18'}) RETURN DISTINCT c"))
    after = time()
    return after - before


print(query_2(sessionDB))

# 3 calls for a specific user within a specific time range


def query_3(session):
    before = time()
    results = list(session.run(
        "MATCH (p:PERSON {number: '351(905)372-6077'})-[:MADE_CALL]->(c:CALL) WHERE toInteger(c.duration) > 4000 RETURN DISTINCT c"))
    after = time()
    return after - before


print(query_3(sessionDB))

# 4 known people from phone number (tu sdt timf tat ca nguoi quen nguoi do)


def query_4(session):
    before = time()
    results = list(session.run(
        "MATCH (p:PERSON)-[k:KNOWS]->(knownpeople:PERSON) WHERE p.number = '504(630)761-4257' RETURN DISTINCT knownpeople"))
    after = time()
    return after - before


print(query_4(sessionDB))


def results_to_csv(results):
    with open('query_results_neo4j.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Query', 'Execution Time'])

        for query, execution_times in results.items():
            for time in execution_times:
                writer.writerow([query, time])


def neo4j_query():
    query1results = []
    query2results = []
    query3results = []
    query4results = []
    queryExecutionTime = {"query1": [],
                          "query2": [], "query3": [], "query4": []}

    for i in range(31):
        query1results.append(query_1(sessionDB))
    for i in range(31):
        query2results.append(query_2(sessionDB))
    for i in range(31):
        query3results.append(query_3(sessionDB))
    for i in range(31):
        query4results.append(query_4(sessionDB))

    queryExecutionTime["query1"] = query1results
    queryExecutionTime["query2"] = query2results
    queryExecutionTime["query3"] = query3results
    queryExecutionTime["query4"] = query4results
    return queryExecutionTime


if __name__ == '__main__':
    results_neo4j = neo4j_query()
    results_to_csv(results_neo4j)
