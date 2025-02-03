from kafka import KafkaProducer
import pymysql
import time
import json
import decimal

# Custom function to handle Decimal types
def decimal_serializer(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Convert Decimal to float
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=decimal_serializer).encode('utf-8')
)


db = pymysql.connect(
    host="127.0.0.1",
    user="newuser",
    password="newpassword",
    database="clothes_db",
    port=3307
)

cursor = db.cursor()
cursor.execute("SELECT * FROM clothes")

while True:
    clothes = cursor.fetchmany(10)
    if not clothes:
        break
    for cloth in clothes:
        producer.send('clothes-topic', cloth)
    time.sleep(10)

cursor.close()
db.close()
