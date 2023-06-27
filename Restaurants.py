import os
import csv
import requests

# Define the REST API URL
url = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_LOS_ANGELES_COUNTY_RESTAURANT_AND_MARKET_INSPECTIONS_/FeatureServer/0/query"

params = {
    "where": "PE_DESCRIPTION LIKE 'RESTAURANT%'",
    "outFields": "*",
    "f": "json",
    "returnGeometry": "false",
    "resultOffset": 0,
    "resultRecordCount": 1000
}

with open("restaurants.csv", "w", newline="") as f:
    writer = None
    while True:
        # Send the request
        response = requests.get(url, params=params)

        # Ensure the request was successful
        response.raise_for_status()

        # Decode the response
        data = response.json()

        # Extract the fields
        field_names = [field['name'] for field in data['fields']]

        # Create the CSV writer
        if writer is None:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()

        # Write the records
        for attributes in data['features']:
            writer.writerow(attributes['attributes'])

        # Check if there are more records
        if len(data['features']) < params["resultRecordCount"]:
            break

        # Get the next chunk of records
        params["resultOffset"] += params["resultRecordCount"]
