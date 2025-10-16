from kafka import KafkaConsumer
import json
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Buffer for rolling statistics
buffer = []

for message in consumer:
    data = message.value
    buffer.append(data)
    
    # Keep last 10 readings for rolling stats
    if len(buffer) > 10:
        buffer.pop(0)
    
    df = pd.DataFrame(buffer)
    
    # Rolling averages
    rolling_avg_temp = df['temperature'].mean()
    rolling_avg_humidity = df['humidity'].mean()
    
    print(f"Rolling Avg Temperature: {rolling_avg_temp:.2f}, Humidity: {rolling_avg_humidity:.2f}")
    
    # Simple ML: Predict next temperature using linear regression
    if len(df) > 2:
        X = np.arange(len(df)).reshape(-1,1)
        y = df['temperature'].values
        model = LinearRegression().fit(X, y)
        prediction = model.predict([[len(df)]])
        print(f"Predicted next temp: {prediction[0]:.2f}")
