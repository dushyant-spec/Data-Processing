from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Read from Kafka
t_env.execute_sql("""
CREATE TABLE sensor_data (
    sensor_id INT,
    temperature DOUBLE,
    humidity DOUBLE,
    timestamp DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-data',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)
""")

# Simple aggregation: rolling average
t_env.execute_sql("""
SELECT
    sensor_id,
    TUMBLE_END(TO_TIMESTAMP_LTZ(timestamp,3), INTERVAL '10' SECOND) as window_end,
    AVG(temperature) as avg_temp,
    AVG(humidity) as avg_humidity
FROM sensor_data
GROUP BY sensor_id, TUMBLE(TO_TIMESTAMP_LTZ(timestamp,3), INTERVAL '10' SECOND)
""").print()
