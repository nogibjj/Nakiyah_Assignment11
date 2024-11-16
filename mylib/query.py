from tabulate import tabulate
import os
import requests
import json
import base64
from extract import extractData
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("queryLog.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")

# # Set environment variables
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# print(os.environ["JAVA_HOME"])

# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("queryLog.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Spark App") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Register the Delta table as a temporary view
delta_table_path = "dbfs:/FileStore/nd191_assignment11/nd191_assignment11_delta_table"
spark.read.format("delta").load(delta_table_path).createOrReplaceTempView("employee_data_delta")

# Now you can run SQL queries on the registered table
def queryData():
    query = """
        SELECT 
            Region, 
            COUNT(Employee_ID) AS Total_High_Stress_Employees
        FROM employee_data_delta
    """

    # Log the query
    logQuery(query)
    query_result = spark.sql(query)
    query_result.show()

queryData()

