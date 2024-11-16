import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/nd191_assignment11"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"
dbfs_path = FILESTORE_PATH + "/Impact_of_Remote_Work_on_Mental_Health.csv"  # The target path in DBFS

# Use the token in the Spark session
spark = SparkSession.builder.appName("Spark App").config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1").getOrCreate()

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
    df_clean = df_clean.withColumn('Access_to_Mental_Health_Resources', 
                                   when(col('Access_to_Mental_Health_Resources') == 'Yes', True).otherwise(False))
    
    # Convert 'Age', 'Years_of_Experience', and 'Hours_Worked_Per_Week' to numeric types
    df_clean = df_clean.withColumn('Age', col('Age').cast('double'))
    df_clean = df_clean.withColumn('Years_of_Experience', col('Years_of_Experience').cast('double'))
    df_clean = df_clean.withColumn('Hours_Worked_Per_Week', col('Hours_Worked_Per_Week').cast('double'))
    df_clean = df_clean.dropna()

    print("Data transformed successfully.")    
    return df_clean

def loadDataToDelta(file_path, delta_table_path):
    try:
        # Load the CSV file from DBFS
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"File loaded successfully from: {file_path}")
        
        # Data transformation (reuse your transformData logic here if needed)
        df_clean = df.select('Employee_ID', 'Age', 'Job_Role', 'Industry', 'Years_of_Experience', 
                             'Work_Location', 'Hours_Worked_Per_Week', 'Mental_Health_Condition', 
                             'Access_to_Mental_Health_Resources')

        df_clean = df_clean.withColumn('Access_to_Mental_Health_Resources', 
                                       when(col('Access_to_Mental_Health_Resources') == 'Yes', True).otherwise(False))
        df_clean = df_clean.withColumn('Age', col('Age').cast('double'))
        df_clean = df_clean.withColumn('Years_of_Experience', col('Years_of_Experience').cast('double'))
        df_clean = df_clean.withColumn('Hours_Worked_Per_Week', col('Hours_Worked_Per_Week').cast('double'))
        df_clean = df_clean.dropna()

        # Write the DataFrame to Delta Lake
        df_clean.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Data successfully written to Delta Lake at: {delta_table_path}")

    except Exception as e:
        print(f"Error while loading data to Delta Lake: {e}")

# Define paths
delta_table_path = "dbfs:/FileStore/nd191_assignment11/nd191_assignment11_delta_table"
dbfs_file_path = "dbfs:/FileStore/nd191_assignment11/Impact_of_Remote_Work_on_Mental_Health.csv"

transformData(dbfs_file_path)
loadDataToDelta(dbfs_file_path, delta_table_path)