import os
from extract import extractData
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Set environment variables
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
print(os.environ["JAVA_HOME"])

# Retrieve Databricks environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")  # Example: 'adb-1234567.89.azuredatabricks.net'
access_token = os.getenv("ACCESS_TOKEN")  # Your Databricks API token

# # Ensure the variables are set
# if not server_h or not access_token:
#     raise ValueError("Databricks credentials (SERVER_HOSTNAME and ACCESS_TOKEN) must be set in the environment variables.")

# # Set the headers and base URL for API calls
# headers = {'Authorization': f'Bearer {access_token}'}
# url = f"https://{server_h}/api/2.0"
# print(f"Databricks URL: {url}")

# Use the token in your Spark session
spark = SparkSession.builder \
    .appName("Spark App") \
    .config("spark.databricks.token", access_token) \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()

# Transform and clean the data using Spark
def transformData(file_path):
    try:
        # Load the CSV file into a Spark DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully: {file_path}")
    except Exception as e:
        print(f"Error loading file: {e}")
        return None
    
    # Select and clean specific columns
    df_clean = df.select('Employee_ID', 'Age', 'Job_Role', 'Industry', 'Years_of_Experience', 
                         'Work_Location', 'Hours_Worked_Per_Week', 'Mental_Health_Condition', 
                         'Access_to_Mental_Health_Resources')
    
    # Convert 'Access_to_Mental_Health_Resources' to boolean
    df_clean = df_clean.withColumn(
        'Access_to_Mental_Health_Resources', 
        when(col('Access_to_Mental_Health_Resources') == 'Yes', True).otherwise(False)
    )
    
    # Convert 'Age', 'Years_of_Experience', and 'Hours_Worked_Per_Week' to numeric types
    df_clean = df_clean.withColumn('Age', col('Age').cast('double'))
    df_clean = df_clean.withColumn('Years_of_Experience', col('Years_of_Experience').cast('double'))
    df_clean = df_clean.withColumn('Hours_Worked_Per_Week', col('Hours_Worked_Per_Week').cast('double'))
    
    # Drop rows with any null values
    df_clean = df_clean.dropna()
    
    print("Data transformed successfully.")
    
    return df_clean

transformData("data/Impact_of_Remote_Work_on_Mental_Health.csv")