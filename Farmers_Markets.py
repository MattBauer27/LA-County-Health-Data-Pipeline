from arcgis.features import FeatureLayer
from arcgis.gis import GIS
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from arcgis.geocoding import geocode
import geopandas as gpd
from shapely.geometry import Point
from arcgis.features import FeatureLayerCollection
import logging
import datetime
import json
import urllib.request
from xml.etree import ElementTree as ET
import pdfplumber
import re

load_dotenv()

# create logger with 'spam_application'
logger = logging.getLogger('run_log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
now = datetime.datetime.now()
logging_date = now.strftime("%Y-%m-%d")
fh = logging.FileHandler(f'Farmers_Markets/RunLog/run_log.csv')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

try:

    # Initialize the GIS
    gis_insp = GIS("https://uscssi.maps.arcgis.com",
                   client_id=os.getenv('client_id'))

    # Define your username and folder
    username = "mkbauer_USCSSI"
    folder_name = os.getenv("folder_name")

    # URL of the PDF
    pdf_url = "https://www.cdfa.ca.gov/is/docs/CurrentMrktsCounty.pdf"

    # Download the PDF
    response = requests.get(pdf_url)
    pdf_path = "CurrentMrktsCounty.pdf"

    with open(pdf_path, "wb") as f:
        f.write(response.content)

    # Open the PDF
    pdf = pdfplumber.open(pdf_path)

    # Initialize variables
    headers = []

    # Iterate over each page in the PDF
    for page in pdf.pages:
        # Extract text from the page
        text = page.extract_text()

        # Extract header from the page
        header = text.split('\n')[:3]
        headers.append(header)

    # Close the PDF
    pdf.close()

    # Remove the PDF
    os.remove(pdf_path)

    # Extract date from header
    # The date is on the second line of the header
    updated_date = headers[1][1]

    # Extract date from the string
    updated_date = updated_date.replace(
        'as of ', '')  # Remove the 'as of ' part

    # Convert to datetime object
    date_obj = datetime.datetime.strptime(updated_date, '%B %d, %Y')

    # Convert to 'YYYY-MM-DD' format
    updated_date = date_obj.strftime('%Y-%m-%d')

    print(updated_date)

    # Check if file exists and get the last update date
    file_path = 'Farmers_Markets/Dates_txts/last_update.txt'
    if os.path.isfile(file_path):
        with open(file_path, 'r') as f:
            last_recorded_dates = [line.strip()
                                   for line in f.readlines()]  # get all lines
    else:
        last_recorded_dates = []

    # If the last update date is more recent than the most recent date in the file, prepend it to the file
    if not last_recorded_dates or updated_date > last_recorded_dates[0]:

        logger.info("Data Updated")

        with open(file_path, 'w') as f:
            f.write(updated_date + '\n')
            for recorded_date in last_recorded_dates:
                f.write(recorded_date + '\n')

        # URL of the PDF
        pdf_url = "https://www.cdfa.ca.gov/is/docs/CurrentMrktsCounty.pdf"

        # Download the PDF
        response = requests.get(pdf_url)
        pdf_path = "CurrentMrktsCounty.pdf"

        with open(pdf_path, "wb") as f:
            f.write(response.content)

        # Open the PDF
        pdf = pdfplumber.open(pdf_path)

        # Initialize variables
        data = []

        # Iterate over each page in the PDF
        for page in pdf.pages:

            # Extract table data from the page
            table = page.extract_table()
            if table:
                data.extend(table)

        # Close the PDF
        pdf.close()

        # Remove the PDF
        os.remove(pdf_path)

        # Filter out rows which don't have 'Los Angeles' in the County Name column
        los_angeles_data = [row for row in data if row[0] == 'Los Angeles']

        # Separate the headers from the data
        headers = data[0]

        # Replace newline characters with a space
        headers = [header.replace('\n', ' ') for header in headers]

        # Create a DataFrame
        new_records_df = pd.DataFrame(los_angeles_data, columns=headers)

        new_records_df.replace('\n', ' ', regex=True, inplace=True)

        # Creating a function which will remove extra leading
        # and tailing whitespace from the data.
        # pass dataframe as a parameter here
        def whitespace_remover(dataframe):

            # iterating over the columns
            for i in dataframe.columns:

                # checking datatype of each columns
                if dataframe[i].dtype == 'object':

                    # applying strip function on column
                    dataframe[i] = dataframe[i].map(str.strip)
                else:

                    # if condn. is False then it will do nothing.
                    pass

        # applying whitespace_remover function on dataframe
        whitespace_remover(new_records_df)

        new_records_df = new_records_df.fillna('None')

        # Remove duplicates
        new_records_df = new_records_df.drop_duplicates(keep='last')

        # Define a geocoding function
        def geocode_address(address):
            # Geocode the address
            geocode_result = geocode(address, as_featureset=False)

            # If a result was found, return the first result's latitude and longitude
            if geocode_result:
                return geocode_result[0]['location']['y'], geocode_result[0]['location']['x']

            # If no result was found, return None
            return None, None

        # Geocode 'Market Location' to get 'Latitude' and 'Longitude'
        new_records_df['Latitude'], new_records_df['Longitude'] = zip(
            *new_records_df['Market Location'].apply(geocode_address))

        new_records_df = new_records_df.fillna('None')

        new_records_df.replace('', 'None', inplace=True)

        new_records_df.to_csv(
            f"Farmers_Markets/Base_csvs/farmers_markets_{updated_date}.csv", index=False, encoding='utf-8')
        if last_recorded_dates:
            old_records_df = pd.read_csv(
                f"Farmers_Markets/Base_csvs/farmers_markets_{last_recorded_dates[0]}.csv", encoding='utf-8')
            old_records_df = old_records_df.fillna('None')

            # Convert all columns to string type
            old_records_df = old_records_df.astype(str)
            new_records_df = new_records_df.astype(str)

            # Dataframe for rows present in new_records_df but not in old_records_df
            df_added = new_records_df[~new_records_df['Market Name'].isin(
                old_records_df['Market Name'])]
            df_added.to_csv(
                f"Farmers_Markets/Added/farmers_markets_added_{updated_date}.csv", index=False, encoding='utf-8')

            # Dataframe for rows present in old_records_df but not in new_records_df
            df_dropped = old_records_df[~old_records_df['Market Name'].isin(
                new_records_df['Market Name'])]
            df_dropped.to_csv(
                f"Farmers_Markets/Dropped/farmers_markets_dropped_{updated_date}.csv", index=False, encoding='utf-8')

            # Compute the number of newly opened and closed facilities
            opened_df = new_records_df[~new_records_df['Market Name'].isin(
                old_records_df['Market Name'])]
            opened = len(opened_df)

            closed_df = old_records_df[~old_records_df['Market Name'].isin(
                new_records_df['Market Name'])]
            closed = len(closed_df)

            # Create new DataFrame to store the result
            result_df = pd.DataFrame(
                {"Date": [updated_date], "Added": [opened], "Removed": [closed]})

            # Define CSV file
            csv_file = "Farmers_Markets/Status_update/Status_updates.csv"

            # If the CSV exists, load it and append the new data
            if os.path.isfile(csv_file):
                df = pd.read_csv(csv_file)
                df = pd.concat([df, result_df])
            else:
                df = result_df

            # Save to CSV
            df.to_csv(csv_file, index=False)

        # Find the feature layer to update
        feature_layer_name = "Farmers_Markets"
        search_result = gis_insp.content.search(
            query=f"title:\"{feature_layer_name}\" AND owner:{username}", item_type="Feature Service")

        # Identify the CSV file to use
        df = pd.read_csv(
            f"Farmers_Markets/Base_csvs/farmers_markets_{updated_date}.csv")

        location_dict = {}
        # Load the existing GeoJSON file
        if last_recorded_dates:
            previous_geojson_file = f"Farmers_Markets/Geocoded_geojson/farmers_markets_{last_recorded_dates[0]}.geojson"
            if os.path.isfile(previous_geojson_file):
                previous_gdf = gpd.read_file(previous_geojson_file)
                location_dict = {f"{row['Market Name']}_{row['Market Location']}": (row['geometry'].y, row['geometry'].x)
                                 for _, row in previous_gdf.iterrows() if row['geometry'] is not None}

        # Transform DataFrame to GeoDataFrame
        geometry = [Point(xy) for xy in zip(
            df.Longitude, df.Latitude)]
        gdf = gpd.GeoDataFrame(df, geometry=geometry)

        # Save GeoDataFrame to GeoJSON
        gdf.to_file(
            f"Farmers_Markets/Geocoded_geojson/farmers_markets_{updated_date}.geojson", driver='GeoJSON')

        # Define the GeoJSON file path
        geojson_file = f"Farmers_Markets/Geocoded_geojson/farmers_markets_{updated_date}.geojson"

        # If service already exists, overwrite its data
        if search_result:
            feature_layer_item = search_result[0]

            # Create a FeatureLayerCollection from the item
            feature_layer_collection = FeatureLayerCollection.fromitem(
                feature_layer_item)

            # Overwrite the feature layer using the GeoJSON file
            feature_layer_collection.manager.overwrite(geojson_file)
        else:
            # If service does not exist publish the GeoJSON file as a new feature layer
            item_properties = {
                "title": feature_layer_name,
                "type": "GeoJson",
                "tags": ["ArcGIS Python API"],
                "description": "Description about the GeoJson File",
            }
            geojson_item = gis_insp.content.add(
                item_properties, geojson_file)
            feature_layer_item = geojson_item.publish()

        # Move the item to a folder
        feature_layer_item.move(folder=folder_name)

        print("Finished updating: {} – ID: {}".format(
            feature_layer_item.title, feature_layer_item.id))

        # Close the file handler
        fh.close()
        # Remove the handler from the logger
        logger.removeHandler(fh)

    else:
        logger.info("No Update")

        # Close the file handler
        fh.close()
        # Remove the handler from the logger
        logger.removeHandler(fh)

except Exception as e:
    logger.error("Exception occurred", exc_info=True)

    # Close the file handler
    fh.close()
    # Remove the handler from the logger
    logger.removeHandler(fh)
