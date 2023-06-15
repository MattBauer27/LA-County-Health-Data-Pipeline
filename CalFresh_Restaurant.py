import os
import requests
import camelot
import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
from dotenv import load_dotenv

load_dotenv()

# Initialize the GIS
gis = GIS("https://uscssi.maps.arcgis.com", client_id=os.getenv('client_id'))

# Define your username and folder
username = "mkbauer_USCSSI"
folder_name = os.getenv("folder_name")

# URL of the PDF file
CalFreshRest_URL = "https://file.lacounty.gov/SDSInter/dpss/1131247_ListingofParticipatingRestaurantsinRMPAugust2022.pdf"

# Path where the PDF file will be saved temporarily
CalFreshRes_PDF = "temp.pdf"

# Download the PDF file
response = requests.get(CalFreshRest_URL)
with open(CalFreshRes_PDF, 'wb') as f:
    f.write(response.content)

# Read the PDF as a list of Table objects
tables = camelot.read_pdf(CalFreshRes_PDF, pages='all')

# Concatenate all the DataFrames into a single DataFrame
df = pd.concat([table.df for table in tables])

# Initialize an empty list to hold dataframes
dfs = []

# Iterate over each column in the DataFrame
for col in df.columns:
    # Split the data into separate columns and combine Address lines
    tmp_df = pd.DataFrame()
    tmp_df['Name'] = df[col].str.split('\n').str[0]
    tmp_df['Address'] = df[col].str.split(
        '\n').str[1] + df[col].str.split('\n').str[2]
    tmp_df['Phone Number'] = df[col].str.split('\n').str[3]

    # Append the temporary DataFrame to the list
    dfs.append(tmp_df)

# Concatenate all the DataFrames in the list into a single DataFrame
CalFreshRest_df = pd.concat(dfs, ignore_index=True)

# Drop the rows with missing values
CalFreshRest_df.dropna()

print(CalFreshRest_df['Name'])

# Geocode the addresses to get the latitude and longitude
CalFreshRest_df['X'] = CalFreshRest_df.apply(lambda row: geocode(f"{row['Name']}, {row['Address']}", out_sr=4326)[
    0]['location']['x'] if geocode(f"{row['Name']}, {row['Address']}") else None, axis=1)
CalFreshRest_df['Y'] = CalFreshRest_df.apply(lambda row: geocode(f"{row['Name']}, {row['Address']}", out_sr=4326)[
    0]['location']['y'] if geocode(f"{row['Name']}, {row['Address']}") else None, axis=1)

# Write the data to a CSV file
CalFreshRest_df.to_csv("CalFreshRestRaw.csv",
                       index=False, encoding='utf-8-sig')

# Check if an item with the same name already exists in your account
CalFreshRest_item = gis.content.search(
    query=f"title:CalFreshRestRaw.csv AND owner:{username}", item_type="CSV")
if CalFreshRest_item:
    # If an item with the same name exists, delete it
    CalFreshRest_item[0].delete()

# Check if a feature layer with the same name already exists in your account
CalFreshRest_layer = gis.content.search(
    query=f"title:CalFresh Restaurants AND owner:{username}", item_type="Feature Layer")
if CalFreshRest_layer:
    # If a feature layer with the same name exists, delete it
    CalFreshRest_layer[0].delete()

# Create a new item in the GIS
CalFreshRest_prop = {'title': 'CalFresh Restaurants'}
CalFreshRest_csv_item = gis.content.add(item_properties=CalFreshRest_prop,
                                        data="CalFreshRestRaw.csv")

# Publish the CSV item as a feature layer
CalFreshRest_feature_layer_item = CalFreshRest_csv_item.publish()

CalFreshRest_feature_layer_item.move(folder_name)

# Delete the local CSV file
os.remove("CalFreshRestRaw.csv")

# Delete the local pdf file
os.remove("temp.pdf")

# Delete the AGOL CSV item
CalFreshRest_csv_item.delete()
