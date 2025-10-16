"""
Incremental Data Processing using CDC events from Kafka
Author: Your Name
Date: 2025-10-16
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ----------------------------
# 1. Setup Flink environment
# ----------------------------
print("[INFO] Initializing Flink streaming environment...")
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# ----------------------------
# 2. Define Kafka CDC Source
# ----------------------------
print("[INFO] Creating CDC source table...")
t_env.execute_sql("""
CREATE TABLE cdc_source (
    id STRING,
    feature1 DOUBLE,
    feature2 DOUBLE,
    op_type STRING METADATA FROM 'op' VIRTUAL,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'cdc_events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
)
""")

# ----------------------------
# 3. Define Output Table (Print Sink)
# ----------------------------
print("[INFO] Creating output table for incremental results...")
t_env.execute_sql("""
CREATE TABLE model_updates (
    id STRING,
    feature1_avg DOUBLE,
    feature2_avg DOUBLE
) WITH ('connector' = 'print')
""")

# ----------------------------
# 4. Incremental aggregation
# ----------------------------
print("[INFO] Starting incremental processing...")
t_env.execute_sql("""
INSERT INTO model_updates
SELECT 
    id,
    AVG(feature1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS feature1_avg,
    AVG(feature2) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS feature2_avg
FROM cdc_source
WHERE op_type <> 'd'
""")

print("[INFO] Flink streaming job deployed successfully!")
