from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import csv

load_dotenv()

# Initialize the GIS
gis = GIS("https://uscssi.maps.arcgis.com", client_id=os.getenv('client_id'))

# URL to your ArcGIS Feature Layer
url = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_COUNTY_OF_LOS_ANGELES_RESTAURANT_AND_MARKET_INVENTORY_/FeatureServer/0"

# Access the feature layer
layer = FeatureLayer(url)

# Fetch metadata about the layer
metadata = layer.properties

# Extract last update date (in UNIX timestamp format)
last_update_unix = metadata.editingInfo.lastEditDate

# Convert UNIX timestamp to readable format
last_update_date = datetime.fromtimestamp(
    last_update_unix / 1000).strftime('%Y-%m-%d')

# Check if file exists and get the last update date
file_path = 'last_update.txt'
if os.path.isfile(file_path):
    with open(file_path, 'r') as f:
        last_recorded_dates = [line.strip()
                               for line in f.readlines()]  # get all lines
else:
    last_recorded_dates = []

# If the last update date is more recent than the most recent date in the file, prepend it to the file
if not last_recorded_dates or last_update_date > last_recorded_dates[0]:
    with open(file_path, 'w') as f:
        f.write(last_update_date + '\n')
        for recorded_date in last_recorded_dates:
            f.write(recorded_date + '\n')

    # Define the REST API URL
    query_url = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_COUNTY_OF_LOS_ANGELES_RESTAURANT_AND_MARKET_INVENTORY_/FeatureServer/0/query"

    params = {
        "where": "PE_DESCRIPTION LIKE 'RESTAURANT%'",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "false",
        "resultOffset": 0,
        "resultRecordCount": 1000
    }

    records = []
    while True:
        # Send the request
        response = requests.get(query_url, params=params)

        # Ensure the request was successful
        response.raise_for_status()

        # Decode the response
        data = response.json()

        # Extract the records
        for attributes in data['features']:
            records.append(attributes['attributes'])

        # Check if there are more records
        if len(data['features']) < params["resultRecordCount"]:
            break

        # Get the next chunk of records
        params["resultOffset"] += params["resultRecordCount"]
        break

    # Convert records to a pandas DataFrame
    new_records_df = pd.DataFrame(records)

    new_records_df.to_csv(
        f"restaurants_{last_update_date}.csv", index=False, encoding='utf-8')
    if last_recorded_dates:
        old_records_df = pd.read_csv(
            f"restaurants_{last_recorded_dates[0]}.csv", encoding='utf-8')

        # Dataframe for rows present in new_records_df but not in old_records_df
        df_added = new_records_df[~new_records_df.apply(
            tuple, 1).isin(old_records_df.apply(tuple, 1))]
        df_added.to_csv(
            f"restaurants_added_{last_update_date}.csv", index=False, encoding='utf-8')

        # Dataframe for rows present in old_records_df but not in new_records_df
        df_dropped = old_records_df[~old_records_df.apply(
            tuple, 1).isin(new_records_df.apply(tuple, 1))]
        df_dropped.to_csv(
            f"restaurants_dropped_{last_update_date}.csv", index=False, encoding='utf-8')
