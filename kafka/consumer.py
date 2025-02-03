from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='multi-topic-group',
    auto_offset_reset='earliest'
)

consumer.subscribe(['clothes-topic', 'users-topic'])

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client['store']


def insert_user_clothes(user_data):
    cloth_ids = user_data['clothIDs']
    clothes = []

    for cloth_id in cloth_ids:
        cloth = db.products.find_one({"clothID": cloth_id})
        if cloth:
            clothes.append(cloth)

    user_clothes_entry = {
        'userID': user_data['userID'],
        'friends': user_data['friends'],
        'collaborators': user_data['collaborators'],
        'clothIDs': user_data['clothIDs'],
        'clothes': clothes
    }

    db.users_clothes.insert_one(user_clothes_entry)


try:
    for message in consumer:
        print(f"Received message from topic {message.topic}: {message.value}")

        if message.topic == 'clothes-topic':
            cloth_data = message.value
            cloth_dict = {
                'clothID': cloth_data[0],
                'style': cloth_data[1],
                'color': cloth_data[2],
                'price': cloth_data[3],
                'brand': cloth_data[4]
            }
            db.products.insert_one(cloth_dict)

        elif message.topic == 'users-topic':
            user_data = message.value
            user_dict = {
                'userID': user_data['userID'],
                'friends': user_data['friends'],
                'collaborators': user_data['collaborators'],
                'clothIDs': user_data['clothIDs']
            }
            db.users.insert_one(user_dict)

            insert_user_clothes(user_data)

except KeyboardInterrupt:
    print("\nConsumer interrupted. Exiting gracefully...")

finally:
    consumer.close()
    print("Consumer closed.")
