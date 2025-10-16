from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# =========================
# ENVIRONMENT SETUP
# =========================
print("Setting up Flink environment...")
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)
print("Environment created.\n")

input_path = r"D:\Data Processing\all_stocks_5.csv"
output_path = r"D:\Data Processing\processed_stocks.csv"

# =========================
# STEP 1: SOURCE TABLE
# =========================
print("Creating source table...")
t_env.execute_sql(f"""
    CREATE TABLE stocks (
        `date` STRING,
        `open` DOUBLE,
        `high` DOUBLE,
        `low` DOUBLE,
        `close` DOUBLE,
        `volume` BIGINT,
        `Name` STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{input_path}',
        'format' = 'csv',
        'csv.ignore-first-line' = 'true',
        'csv.ignore-parse-errors' = 'true'
    )
""")
print("Source table created.\n")

# =========================
# 1️⃣ HANDLE MISSING VALUES
# =========================
print("Removing rows with missing values...")
t_env.execute_sql("""
    CREATE TEMPORARY VIEW stocks_clean AS
    SELECT *
    FROM stocks
    WHERE `open` IS NOT NULL
      AND `high` IS NOT NULL
      AND `low` IS NOT NULL
      AND `close` IS NOT NULL
      AND `volume` IS NOT NULL
""")
print("Missing values removed.\n")

# =========================
# 2️⃣ REMOVE DUPLICATES
# =========================
print("Removing duplicate rows...")
t_env.execute_sql("""
    CREATE TEMPORARY VIEW stocks_unique AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY `date`, `Name` ORDER BY `date`) AS rn
        FROM stocks_clean
    )
    WHERE rn = 1
""")
print("Duplicates removed.\n")

# =========================
# 3️⃣ FEATURE ENGINEERING
# =========================
print("Calculating new features (daily_range, norm_close)...")
t_env.execute_sql("""
    CREATE TEMPORARY VIEW stocks_features AS
    SELECT *,
           (`high` - `low`) AS daily_range,
           CASE WHEN (`high` - `low`) <> 0 THEN (`close` - `low`) / (`high` - `low`) ELSE 0 END AS norm_close
    FROM stocks_unique
""")
print("Feature engineering completed.\n")

# =========================
# 4️⃣ NORMALIZATION/SCALING
# =========================
print("Applying Min-Max normalization to volume column...")
t_env.execute_sql("""
    CREATE TEMPORARY VIEW stocks_scaled AS
    SELECT *,
           (volume - MIN(volume) OVER()) / (MAX(volume) OVER() - MIN(volume) OVER()) AS volume_scaled
    FROM stocks_features
""")
print("Normalization completed.\n")

# =========================
# 5️⃣ DATA TYPE CONSISTENCY
# =========================
print("Converting date column to DATE type...")
t_env.execute_sql("""
    CREATE TEMPORARY VIEW stocks_final AS
    SELECT 
        TO_DATE(`date`, 'yyyy-MM-dd') AS trade_date,
        `open`, `high`, `low`, `close`, 
        `volume`, `Name`, 
        daily_range, norm_close, volume_scaled
    FROM stocks_scaled
""")
print("Data types adjusted.\n")

# =========================
# SINK TABLE
# =========================
print("Creating sink table for processed data...")
t_env.execute_sql(f"""
    CREATE TABLE processed_stocks (
        trade_date DATE,
        `open` DOUBLE,
        `high` DOUBLE,
        `low` DOUBLE,
        `close` DOUBLE,
        `volume` BIGINT,
        `Name` STRING,
        daily_range DOUBLE,
        norm_close DOUBLE,
        volume_scaled DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'csv'
    )
""")
print("Sink table created.\n")

# =========================
# INSERT FINAL DATA
# =========================
print("Submitting final job to Flink...")
t_env.execute_sql("INSERT INTO processed_stocks SELECT * FROM stocks_final")
print("Job submitted successfully! Check output CSV for results.")
