from kafka import KafkaProducer
from neo4j import GraphDatabase
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

def get_users(tx, skip, limit):
    query = f"""
    MATCH (u:User)
    OPTIONAL MATCH (u)-[:FRIEND]->(friend:User)
    OPTIONAL MATCH (u)-[:COLLABORATOR]->(collaborator:User)
    RETURN u.userID AS userID, u.clothID AS clothIDs, 
           COLLECT(friend.userID) AS friends, 
           COLLECT(collaborator.userID) AS collaborators
    SKIP {skip} LIMIT {limit}
    """
    result = tx.run(query)

    users = []
    for record in result:
        user_data = {
            'userID': record['userID'],
            'clothIDs': record['clothIDs'],
            'friends': record['friends'],
            'collaborators': record['collaborators']
        }
        users.append(user_data)
    return users

with driver.session() as session:
    skip = 0
    limit = 5
    while True:
        users = session.execute_read(get_users, skip, limit)
        if not users:
            print("No more users found. Exiting.")
            break
        for user in users:
            producer.send('users-topic', user)
        time.sleep(20)
        skip += limit

driver.close()
