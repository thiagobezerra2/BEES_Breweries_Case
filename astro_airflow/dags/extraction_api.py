import requests
import json
import time
import os


# Extract data from Open Brewery DB API
 
def fetch_data():
    url = "https://api.openbrewerydb.org/breweries"
    max_retries = 5
    for attempt in range(max_retries):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            time.sleep(2** attempt)
    raise Exception(f"Failed to fetch data{max_retries}")

# Save Raw data
def raw_data(data):
    os.makedirs('data/bronze', exist_ok=True)
    with open('data/bronze/brewery_data.jseon', 'w') as f:
        json.dump(data, f)

if __name__ == '__main__':
    data = fetch_data()
    raw_data(data)