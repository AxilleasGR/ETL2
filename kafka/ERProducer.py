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

# Kafka Producer setup with custom serializer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=decimal_serializer).encode('utf-8')
)

# MySQL connection using pymysql and Docker's IP or localhost
db = pymysql.connect(
    host="127.0.0.1",  # Use the host or Docker IP address if needed
    user="newuser",
    password="newpassword",  # Use your correct root password
    database="clothes_db",
    port=3307  # Ensure the port is correct
)

# Execute SQL query and produce messages
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
