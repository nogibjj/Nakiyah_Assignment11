"""
Test databricks fucntionaility
"""
# test_main.py
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/nd191_assignment11"
url = f"https://{server_h}/api/2.0"


def check_filestore_path(path, headers): 
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get('path') is not None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return False

def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(FILESTORE_PATH, headers) is True, "Filestore path does not exist or is inaccessible."

if __name__ == "__main__":
    print("Testing Databricks")
    test_databricks()
    print("Test worked")