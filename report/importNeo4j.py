from neo4j import GraphDatabase
import os 
from time import time

url = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
neo4jVersion = os.getenv("NEO4J_VERSION", "5")
database = ("Database Mod B", "neo4j")

driver = GraphDatabase.driver(url, auth=(username, password))
sessionDB = driver.session()


# Load data to nodes
sessionDB.run("""LOAD CSV WITH HEADERS FROM "file:///call_record.csv" AS line
                MERGE (a:PERSON {number: line.CALLING_NUMBER})
                ON CREATE SET a.first_name = line.FIRST_NAME, a.last_name = line.LAST_NAME, a.full_name = line.FULL_NAME
                ON MATCH SET a.first_name = line.FIRST_NAME, a.last_name = line.LAST_NAME, a.full_name = line.FULL_NAME
                MERGE (b:PERSON {number: line.CALLED_NUMBER})
                MERGE (c:CALL {id: line.ID})
                ON CREATE SET c.start = toInteger(line.START_DATE), c.end= toInteger(line.END_DATE), c.duration = line.DURATION
                MERGE (d:LOCATION {cell_site: line.CELL_SITE})
                ON CREATE SET d.address= line.ADDRESS, d.state = line.STATE, d.city = line.CITY
                MERGE (e:CITY {name: line.CITY})
                MERGE (f:STATE {name: line.STATE});
                """)
sessionDB.run("""LOAD CSV WITH HEADERS FROM "file:///people_data.csv" AS line
                MERGE (a:PERSON {number: line.phone_number})
                ON CREATE SET a.first_name = line.fist_name, a.last_name = line.last_name, a.full_name = line.full_name
                ON MATCH SET a.first_name = line.first_name, a.last_name = line.last_name, a.full_name = line.full_name;
                """)


# Create relationships between people and calls
sessionDB.run("""LOAD CSV WITH HEADERS FROM "file:///call_record.csv" AS line
                MATCH (a:PERSON {number: line.CALLING_NUMBER}),(b:PERSON {number: line.CALLED_NUMBER}),(c:CALL {id: line.ID})
                CREATE (a)-[:MADE_CALL]->(c)-[:RECEIVED_CALL]->(b);
                """)

# Create relationships between calls and locations
sessionDB.run("""LOAD CSV WITH HEADERS FROM "file:///call_record.csv" AS line
                MATCH (a:CALL {id: line.ID}), (b:LOCATION {cell_site: line.CELL_SITE})
                CREATE (a)-[:LOCATED_IN]->(b);
                """)

# Create relationships between locations, cities and states
sessionDB.run("""LOAD CSV WITH HEADERS FROM "file:///call_record.csv" AS line
                MATCH (a:LOCATION {cell_site: line.CELL_SITE}), (b:STATE {name: line.STATE}), (c:CITY {name: line.CITY})
                CREATE (b)<-[:HAS_STATE]-(a)-[:HAS_CITY]->(c);
                """)

# To delete duplicate relationship between LOCATION and CITY
sessionDB.run("""MATCH (a:LOCATION)-[r:HAS_CITY]->(c:CITY)
                WITH a,c,type(r) as t, tail(collect(r)) as coll
                FOREACH(x in coll | delete x);
                """)

# To delete duplicate relationship between LOCATION and STATE
sessionDB.run("""MATCH (a:LOCATION)-[r:HAS_STATE]->(c:STATE)
                WITH a,c,type(r) as t, tail(collect(r)) as coll
                FOREACH(x in coll | delete x);
                """)

# To create KNOWS relationship between all proper nodes
sessionDB.run("""MATCH (caller:PERSON)-[:MADE_CALL]->(call:CALL)-[:RECEIVED_CALL]->(receiver:PERSON)
                MERGE (caller)-[:KNOWS]->(receiver);
                """)