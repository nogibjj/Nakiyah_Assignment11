from extract import extractData

# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("queryLog.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")

# Register the Delta table as a temporary view
# delta_table_path = "dbfs:/FileStore/nd191_assignment11/nd191_assignment11_delta_table"
# spark.read.format("delta").load(delta_table_path).createOrReplaceTempView("employee_data_delta")

# Now you can run SQL queries on the registered table
def queryData():
    query = """
        SELECT *
        FROM employee_data_delta
    """
    # Log the query
    logQuery(query)
    query_result = spark.sql(query)
    query_result.show()

queryData()

