# in_memory_analytics.py
# Apache Spark In-Memory Data Processing Demo
# Author: Your Name
# Date: 2025-10-16

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr
import time

def main():
    # ============================
    # 1. Initialize Spark Session
    # ============================
    spark = SparkSession.builder \
        .appName("InMemoryDataProcessing") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    print("Spark Session Initialized!\n")

    # ===================================
    # 2. Create Sample DataFrame (In-Memory)
    # ===================================
    data = [
        (1, "Alice", 23, 3000),
        (2, "Bob", 30, 4000),
        (3, "Charlie", 28, 3500),
        (4, "David", 35, 5000),
        (5, "Eva", 26, 4500)
    ]
    columns = ["id", "name", "age", "salary"]

    df = spark.createDataFrame(data, columns)
    df.cache()  # Keep DataFrame in memory for repeated queries
    print("DataFrame Loaded and Cached in Memory:")
    df.show()

    # ===================================
    # 3. Perform In-Memory Analytics
    # ===================================

    # 3a. Aggregation: Average salary
    print("Average Salary:")
    avg_salary = df.agg(avg("salary").alias("avg_salary"))
    avg_salary.show()

    # 3b. Filtering: Employees with salary > 4000
    print("Employees with Salary > 4000:")
    high_salary = df.filter(col("salary") > 4000)
    high_salary.show()

    # 3c. Performance Improvement Demonstration
    print("Demonstrating caching performance...")
    start_time = time.time()
    df.filter(col("age") > 25).count()
    first_run = time.time() - start_time

    start_time = time.time()
    df.filter(col("age") > 25).count()
    second_run = time.time() - start_time

    print(f"First run (without cache effect): {first_run:.5f}s")
    print(f"Second run (cached in-memory): {second_run:.5f}s\n")

    # ===================================
    # 4. RDD Operations
    # ===================================
    print("RDD-based filtering and mapping:")
    rdd = df.rdd
    result_rdd = rdd.filter(lambda x: x['salary'] > 3500).map(lambda x: (x['name'], x['salary']))
    for item in result_rdd.collect():
        print(item)

    # ===================================
    # 5. Optional: Streaming Demo
    # ===================================
    print("\nSimulating simple streaming analytics (runs 10 seconds)...")
    stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    stream_df = stream_df.withColumn("value_squared", expr("value * value"))

    query = stream_df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination(10)  # Run for 10 seconds
    query.stop()

    # Stop Spark session
    spark.stop()
    print("\nSpark Session Stopped. Processing Completed!")

if __name__ == "__main__":
    main()
