import requests
from datetime import datetime
import os

# URL to your ArcGIS Feature Layer metadata (add "?f=json" to get JSON format)
url = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_LOS_ANGELES_COUNTY_RESTAURANT_AND_MARKET_INSPECTIONS_/FeatureServer/0?f=json"

response = requests.get(url)

# Ensure the request was successful
response.raise_for_status()

# Decode the response
data = response.json()

# Extract last update date (in UNIX timestamp format)
last_update_unix = data['editingInfo']['lastEditDate']

# Convert UNIX timestamp to readable format
last_update_date = datetime.fromtimestamp(
    last_update_unix / 1000).strftime('%Y-%m-%d')

# Check if file exists and get the last update date
file_path = 'last_update.txt'
if os.path.isfile(file_path):
    with open(file_path, 'r') as f:
        last_recorded_date = f.readlines()[-1].strip()  # get the last line
else:
    last_recorded_date = None

# If the last update date is different from the last recorded date, append it to the file
if last_update_date != last_recorded_date:
    with open(file_path, 'a') as f:
        f.write(last_update_date + '\n')
