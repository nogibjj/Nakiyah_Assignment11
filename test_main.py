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
        full_url = url + f"/dbfs/get-status?path={path}"
        print(f"Checking URL: {full_url}")
        response = requests.get(full_url, headers=headers)
        response.raise_for_status()
        return response.json()['path'] is not None
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False

def test_databricks():
    if not server_h or not access_token:
        raise ValueError("SERVER_HOSTNAME or ACCESS_TOKEN environment variable is missing.")
    
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(FILESTORE_PATH, headers) is True, (
        f"Filestore path does not exist or is inaccessible. "
        f"Hostname: {server_h}, Path: {FILESTORE_PATH}"
    )
