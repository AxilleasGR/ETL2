from kafka import KafkaProducer
from neo4j import GraphDatabase
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

def get_users(tx):
    result = tx.run("MATCH (u:User) RETURN u")
    users = []
    for record in result:
        # Extract the properties of the Node using _properties
        user_data = record['u']._properties
        users.append(user_data)
    return users

with driver.session() as session:
    while True:
        users = session.execute_read(get_users)
        for user in users:
            producer.send('users-topic', user)  # Send user data to Kafka
        time.sleep(20)

driver.close()
