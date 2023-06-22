import csv
import requests

# Define the REST API URL
url = "https://chronicdata.cdc.gov/resource/cwsq-ngmh.json"

with open("mental_health.csv", "w", newline="") as f:
    writer = None
    offset = 0
    limit = 1000
    while True:
        # Set the request parameters
        params = {
            "countyname": "Los Angeles",
            "measure": "Mental health not good for >=14 days among adults aged >=18 years",
            "$offset": offset,
            "$limit": limit
        }

        # Send the request
        response = requests.get(url, params=params)

        # Ensure the request was successful
        response.raise_for_status()

        # Decode the response
        data = response.json()

        # If there's no data, break the loop
        if not data:
            break

        # Extract the field names
        if writer is None:
            field_names = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()

        # Write the records
        for record in data:
            writer.writerow(record)

        # Check if there are more records
        if len(data) < 1:
            break

        # Get the next chunk of records
        offset += limit
