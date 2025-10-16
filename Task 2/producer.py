from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    sensor_data = {
        'sensor_id': random.randint(1, 5),
        'temperature': round(random.uniform(20, 40), 2),
        'humidity': round(random.uniform(30, 80), 2),
        'timestamp': time.time()
    }
    producer.send('sensor-data', sensor_data)
    print(f"Produced: {sensor_data}")
    time.sleep(1)
