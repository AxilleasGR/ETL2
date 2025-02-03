from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Consumer setup
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='multi-topic-group',
    auto_offset_reset='earliest'
)

# Subscribe to multiple topics
consumer.subscribe(['clothes-topic', 'users-topic'])

# MongoDB Connection
client = MongoClient('mongodb://root:root@localhost:27017/')
db = client['store']

try:
    for message in consumer:
        print(f"Received message from topic {message.topic}: {message.value}")
        if message.topic == 'clothes-topic':
            # Transform the list into a dictionary
            keys = ['id', 'type', 'color', 'price', 'brand']  # Adjust keys as per your schema
            cloth_dict = dict(zip(keys, message.value))
            db.products.insert_one(cloth_dict)
        elif message.topic == 'users-topic':
            # Ensure the message for users-topic is also in dictionary format
            db.users.insert_one(message.value)
except KeyboardInterrupt:
    print("\nConsumer interrupted. Exiting gracefully...")
finally:
    consumer.close()
    print("Consumer closed.")
