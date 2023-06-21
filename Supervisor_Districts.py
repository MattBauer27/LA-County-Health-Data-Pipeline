import os
import requests

# Define the REST API URL
url = "https://public.gis.lacounty.gov/public/rest/services/LACounty_Dynamic/Political_Boundaries/MapServer/27/query"

params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "returnGeometry": "true",
}

# Send the request
response = requests.get(url, params=params)

# Ensure the request was successful
response.raise_for_status()

# Write the GeoJSON data to a file
with open("supervisory_districts.geojson", "w") as f:
    f.write(response.text)
