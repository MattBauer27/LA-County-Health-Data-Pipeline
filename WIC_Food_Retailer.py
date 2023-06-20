import json
import urllib.request
import pandas as pd
import os
import requests
import camelot
from arcgis.gis import GIS
from arcgis.geocoding import geocode
from dotenv import load_dotenv
from arcgis.features import FeatureLayerCollection

load_dotenv()

# Initialize the GIS
gis = GIS("https://uscssi.maps.arcgis.com", client_id=os.getenv('client_id'))

# Define your username and folder
username = "mkbauer_USCSSI"
folder_name = os.getenv("folder_name")

url = 'https://data.chhs.ca.gov/api/3/action/datastore_search?resource_id=ee10b67b-2b93-47e7-aa41-cecfbbd32e17&limit=100'
offset = 0
records = []

while True:
    response = urllib.request.urlopen(f"{url}&offset={offset}")
    response_dict = json.loads(response.read())
    records_batch = response_dict['result']['records']

    # If no more records, break the loop
    if not records_batch:
        break

    records.extend(records_batch)
    offset += len(records_batch)

# Load the records into a DataFrame
df1 = pd.DataFrame(records)

# Clean all whitespace and newlines from all columns
df1 = df1.applymap(lambda x: x.strip().replace(
    '\n', ' ') if isinstance(x, str) else x)

""" # Limit the number of rows in df1 for testing
df1 = df1.head(500) """

# Check if the necessary columns exist in df1
assert '_id' in df1.columns, "Column '_id' not found in df1"
assert 'ADDRESS' in df1.columns, "Column 'ADDRESS' not found in df1"
assert 'VENDOR' in df1.columns, "Column 'VENDOR' not found in df1"

# Drop the unnecessary columns
df1 = df1.drop(columns=['_id'])

df1 = df1.dropna(subset=['ADDRESS'])

# Export the DataFrame to a CSV file
df1.to_csv('records.csv', index=False, encoding='utf-8-sig')

# URL of the PDF file
WICCard_URL = "https://www.cdph.ca.gov/Programs/CFH/DWICSN/CDPH%20Document%20Library/WICCard/Stores%20Accepting%20California%20WIC%20Card.pdf"

# Path where the PDF file will be saved temporarily
WICCard_PDF = "temp.pdf"

# Download the PDF file
response = requests.get(WICCard_URL)
with open(WICCard_PDF, 'wb') as f:
    f.write(response.content)

# Read the PDF as a list of Table objects
tables = camelot.read_pdf(WICCard_PDF, pages='all')

# Concatenate all the DataFrames into a single DataFrame
df2 = pd.concat([table.df for table in tables])

# Set the first row as the column names
df2.columns = df2.iloc[0]

# Remove the first row
df2 = df2[1:]

# Identify the rows that are identical to the column headers
header_rows = df2.eq(df2.columns)

# Check if all values in a row are True (i.e., the row is a header row)
header_rows = header_rows.all(axis=1)

# Keep only the rows that are not header rows
df2 = df2[~header_rows]

# Clean all whitespace and newlines from all columns
df2 = df2.applymap(lambda x: x.strip().replace(
    '\n', ' ') if isinstance(x, str) else x)

# Replace all whitespace characters with a single space in column names
df2.columns = df2.columns.str.replace(r'\s+', ' ', regex=True).str.strip()

""" # Limit the number of rows in df2 for testing
df2 = df2.head(500) """

# Check if the necessary columns exist in df2
assert 'STORE NAME' in df2.columns, "Column 'STORE NAME' not found in df2"
assert 'STORE ADDRESS' in df2.columns, "Column 'STORE ADDRESS' not found in df2"
assert 'STORE PHONE NUMBER' in df2.columns, "Column 'STORE PHONE NUMBER' not found in df2"

# Export the DataFrame to a CSV file
df2.to_csv('records2.csv', index=False, encoding='utf-8-sig')


# Add this line before the merge operation
df1['STORE PHONE NUMBER'] = None

df1['ADDRESS'] = df1['ADDRESS'].str.strip().str.replace(r'\s+', ' ', regex=True)

# Identify addresses that appear more than once in either DataFrame
duplicate_addresses_df1 = df1['ADDRESS'].value_counts() > 1
duplicate_addresses_df2 = df2['STORE ADDRESS'].value_counts() > 1

duplicate_addresses = set(duplicate_addresses_df1.index[duplicate_addresses_df1]) | set(
    duplicate_addresses_df2.index[duplicate_addresses_df2])

# Create a mask for rows in df1 that have a duplicate address
mask_df1 = df1['ADDRESS'].isin(duplicate_addresses)

# Create a mask for rows in df2 that have a duplicate address
mask_df2 = df2['STORE ADDRESS'].isin(duplicate_addresses)

# Add this line before the merge operation
df1['STORE PHONE NUMBER'] = None

# Identify rows in df1 and df2 that have a duplicate address
mask_df1 = df1['ADDRESS'].duplicated(keep=False)
mask_df2 = df2['STORE ADDRESS'].duplicated(keep=False)

# Merge on 'ADDRESS' and 'STORE ADDRESS' only for rows that do not have a duplicate address
merged_df1 = df1[~mask_df1].merge(df2[~mask_df2][['STORE ADDRESS', 'STORE PHONE NUMBER']],
                                  left_on='ADDRESS', right_on='STORE ADDRESS', how='left').drop(columns=['STORE PHONE NUMBER_x'])

# For rows that have a duplicate address, merge on 'ADDRESS', 'STORE ADDRESS', 'VENDOR', and 'STORE NAME'
merged_df2 = df1[mask_df1].merge(df2[mask_df2][['STORE ADDRESS', 'STORE NAME', 'STORE PHONE NUMBER']], left_on=[
                                 'ADDRESS', 'VENDOR'], right_on=['STORE ADDRESS', 'STORE NAME'], how='left').drop(columns=['STORE PHONE NUMBER_x'])

# Concatenate the two DataFrames
merged_df = pd.concat([merged_df1, merged_df2])

# Reset the index
merged_df.reset_index(drop=True, inplace=True)

# Directly assign the 'STORE PHONE NUMBER' column in df1 with the 'STORE PHONE NUMBER_y' column in merged_df
df1['STORE PHONE NUMBER'] = merged_df['STORE PHONE NUMBER_y']

# Convert the 'ZIP' column to integers
df1['ZIP'] = df1['ZIP'].astype(int)

# Concatenate 'ADDRESS', 'SECOND ADDRESS', 'CITY', and 'ZIP' into a single string
df1['ADDRESS'] = df1['ADDRESS'] + ', ' + df1['SECOND ADDRESS'] + \
    ', ' + df1['CITY'] + ', ' + df1['ZIP'].astype(str)

# Convert the string to title case
df1['ADDRESS'] = df1['ADDRESS'].str.title()

# Convert the string to title case
df1['VENDOR'] = df1['VENDOR'].str.title()

df1 = df1.drop(columns=['CITY', 'SECOND ADDRESS', 'ZIP'])

# Reorder the columns
df1 = df1[['VENDOR', 'ADDRESS', 'COUNTY',
           'STORE PHONE NUMBER', 'LATITUDE', 'LONGITUDE']]

# Rename the columns and convert to title case
df1 = df1.rename(columns={
    'VENDOR': 'Name',
    'ADDRESS': 'Address',
    'COUNTY': 'County',
    'STORE PHONE NUMBER': 'Phone Number',
    'LATITUDE': 'Latitude',
    'LONGITUDE': 'Longitude'
})

# Export the DataFrame to a CSV file
df1.to_csv('records3.csv', index=False, encoding='utf-8-sig')

for i, row in df1[df1['Latitude'].isnull() | df1['Longitude'].isnull()].iterrows():
    try:
        geocoded = geocode(row['Address'])[0]
        df1.at[i, 'Latitude'] = geocoded['location']['y']
        df1.at[i, 'Longitude'] = geocoded['location']['x']
    except IndexError:
        print(f"Could not geocode address: {row['Address']}")

# Define the name of the feature layer
feature_layer_name = 'WIC Food Retailer'

# Search for an existing feature layer with the same name
search_result = gis.content.search(
    query=f'title:{feature_layer_name} AND owner:{username}', item_type='Feature Layer')

# Export the DataFrame to a CSV file
csv_file = 'records_final.csv'
df1.to_csv(csv_file, index=False)

# Define the name of the CSV item
csv_item_name = 'records_final'

# Search for an existing CSV item with the same name
search_result = gis.content.search(
    query=f'title:{csv_item_name} AND owner:{username}', item_type='CSV')

# If the CSV item exists, delete it
if search_result:
    for item in search_result:
        item.delete()

# If the feature layer exists, overwrite it
if search_result:
    # Assume the first search result is the correct item
    feature_layer_item = search_result[0]

    # Upload the CSV file to ArcGIS Online with the specified title
    csv_item = gis.content.add({'title': feature_layer_name}, csv_file)

    # Publish the CSV file as a hosted feature layer
    new_feature_layer_item = csv_item.publish()

    # Create a FeatureLayerCollection from the item
    feature_layer_collection = FeatureLayerCollection.fromitem(
        feature_layer_item)

    # Overwrite the feature layer with the new feature layer
    feature_layer_collection.manager.update(
        {'url': new_feature_layer_item.url})

    # Delete the new feature layer item and the CSV item
    new_feature_layer_item.delete()
    csv_item.delete()
else:
    # Upload the CSV file to ArcGIS Online with the specified title
    csv_item = gis.content.add({'title': feature_layer_name}, csv_file)

    # Publish the CSV file as a hosted feature layer
    feature_layer_item = csv_item.publish()

    # Move the item to a folder
    feature_layer_item.move(folder=folder_name)

    # Delete the CSV item
    csv_item.delete()
