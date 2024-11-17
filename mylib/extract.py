import requests
import os

# Extracting data from GitHub
def extractData(file_path, url="https://raw.githubusercontent.com/viraterletska/Impact_of_Remote_Work_on_Mental_Health/main/data/Impact_of_Remote_Work_on_Mental_Health.csv"):

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Request data from URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded successfully: {file_path}")
        return file_path
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        return None