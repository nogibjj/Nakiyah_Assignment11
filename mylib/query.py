from tabulate import tabulate

# Markdown file to log the SQL functions and queries
def logQuery(query):
    with open("queryLog.md", "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")

# Query the top 20 records
def queryData(n): 
    connection = sqlite3.connect("database1.db")
    cursor = connection.cursor()
    query = f"SELECT * FROM worker_health LIMIT {n}"
    cursor.execute(query)
    print(f"Top {n} rows of the worker_health table:")
    results = cursor.fetchall()
    headers = [description[0] for description in cursor.description] # get headers
    # for row in results:
        # print(row)
    table = tabulate(results, headers=headers, tablefmt='pretty') # Create the table using tabulate
    print(f"Top {n} rows of wokrker_health table")
    print(table)
    connection.close()
    logQuery(query)  # Log the query in the md file
