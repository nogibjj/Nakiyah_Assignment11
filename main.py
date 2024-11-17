from mylib.extract import extractData
from mylib.transform import transformData, loadDataToDBFS, loadDataToDelta
from mylib.query import queryData
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


if __name__ == "__main__":    
    # Use the token in the Spark session
    spark = SparkSession.builder.appName("Spark App").config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1").getOrCreate()

    # Load environment variables
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    FILESTORE_PATH = "dbfs:/FileStore/nd191_assignment11"
    headers = {'Authorization': 'Bearer %s' % access_token}
    url = "https://"+server_h+"/api/2.0"

    # Define paths
    dbfs_path = FILESTORE_PATH + "/Impact_of_Remote_Work_on_Mental_Health.csv"  # The target path in DBFS
    delta_table_path = "dbfs:/FileStore/nd191_assignment11/nd191_assignment11_delta_table"
    dbfs_file_path = "dbfs:/FileStore/nd191_assignment11/Impact_of_Remote_Work_on_Mental_Health.csv"
    local_file_path = "data/Impact_of_Remote_Work_on_Mental_Health.csv"

    extractData(local_file_path)
    transformData(spark, dbfs_file_path)
    loadDataToDBFS(local_file_path, dbfs_path, headers)
    loadDataToDelta(spark, dbfs_file_path, delta_table_path)
    queryData()