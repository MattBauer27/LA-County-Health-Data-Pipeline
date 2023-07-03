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
import requests
from xml.etree import ElementTree as ET

load_dotenv()

# create logger with 'spam_application'
logger = logging.getLogger('run_log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
now = datetime.datetime.now()
logging_date = now.strftime("%Y-%m-%d")
fh = logging.FileHandler(f'WIC_Food_Retailers/RunLog/run_log.csv')
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

    url = 'https://data.chhs.ca.gov/datastore/odata3.0/ee10b67b-2b93-47e7-aa41-cecfbbd32e17?$top=1&$format=xml'
    ns = {'default': 'http://www.w3.org/2005/Atom'}  # define namespace

    response = urllib.request.urlopen(url)
    data = response.read()  # a `bytes` object
    xml_content = data.decode('utf-8')

    root = ET.fromstring(xml_content)
    updated_element = root.find('default:updated', ns)
    if updated_element is not None:
        # remove the last character 'Z'
        updated_text = updated_element.text[:-1]
        updated_datetime = datetime.datetime.strptime(
            updated_text, "%Y-%m-%dT%H:%M:%S.%f")
        updated_date = updated_datetime.date()  # Convert to date
        updated_date = updated_date.strftime("%Y-%m-%d")
        print(updated_date)
    else:
        print('updated field not found')

    # Check if file exists and get the last update date
    file_path = 'WIC_Food_Retailers/Dates_txts/last_update.txt'
    if os.path.isfile(file_path):
        with open(file_path, 'r') as f:
            last_recorded_dates = [line.strip()
                                   for line in f.readlines()]  # get all lines
    else:
        last_recorded_dates = []

    # If the last update date is more recent than the most recent date in the file, prepend it to the file
    if not last_recorded_dates or updated_date > last_recorded_dates[0]:
        with open(file_path, 'w') as f:
            f.write(updated_date + '\n')
            for recorded_date in last_recorded_dates:
                f.write(recorded_date + '\n')

        url = 'https://data.chhs.ca.gov/api/3/action/datastore_search?q=Los_Angeles&resource_id=ee10b67b-2b93-47e7-aa41-cecfbbd32e17&limit=100'
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

        # Drop the unnecessary columns
        df1 = df1.drop(columns=['_id'])

        # Convert records to a pandas DataFrame
        new_records_df = pd.DataFrame(records_insp)
        # new_records_df = new_records_df.fillna('None')

        new_records_df.to_csv(
            f"WIC_Food_Retailers/Base_csvs/wic_food_retailers_{updated_date}.csv", index=False, encoding='utf-8')
        if last_recorded_dates:
            old_records_df = pd.read_csv(
                f"WIC_Food_Retailers/Base_csvs/wic_food_retailers_{last_recorded_dates[0]}.csv", encoding='utf-8')

            # old_records_df = old_records_df.fillna('None')

            # Convert all columns to string type
            old_records_df = old_records_df.astype(str)
            new_records_df = new_records_df.astype(str)

            # Dataframe for rows present in new_records_df but not in old_records_df
            df_added = new_records_df[~new_records_df.apply(
                tuple, 1).isin(old_records_df.apply(tuple, 1))]
            df_added.to_csv(
                f"WIC_Food_Retailers/Added/wic_food_retailers_{updated_date}.csv", index=False, encoding='utf-8')

            # Dataframe for rows present in old_records_df but not in new_records_df
            df_dropped = old_records_df[~old_records_df.apply(
                tuple, 1).isin(new_records_df.apply(tuple, 1))]
            df_dropped.to_csv(
                f"WIC_Food_Retailers/Dropped/wic_food_retailers_{updated_date}.csv", index=False, encoding='utf-8')

        logger.info("Data Updated")

        # Compute the number of newly opened and closed facilities
        opened_df = new_records_df[~new_records_df['VENDOR'].isin(
            old_records_df['VENDOR'])]
        opened = len(opened_df)

        closed_df = old_records_df[~old_records_df['VENDOR'].isin(
            new_records_df['VENDOR'])]
        closed = len(closed_df)

        # Create new DataFrame to store the result
        result_df = pd.DataFrame(
            {"Date": [update_date], "Opened": [opened], "Closed": [closed]})

        # Define CSV file
        csv_file = "WIC_Food_Retailers/Status_update/Status_updates.csv"

        # If the CSV exists, load it and append the new data
        if os.path.isfile(csv_file):
            df = pd.read_csv(csv_file)
            df = df.append(result_df)
        else:
            df = result_df

        # Save to CSV
        df.to_csv(csv_file, index=False)

        # Find the feature layer to update
        feature_layer_name = "WIC_Food_Retailers"
        search_result = gis_insp.content.search(
            query=f"title:\"{feature_layer_name}\" AND owner:{username}", item_type="Feature Service")

        location_dict = {}
        # Load the existing GeoJSON file
        if last_recorded_dates:
            previous_geojson_file = f"WIC_Food_Retailers/Geocoded_geojson/wic_food_retailers_{last_recorded_dates[0]}.geojson"
            if os.path.isfile(previous_geojson_file):
                previous_gdf = gpd.read_file(previous_geojson_file)

                # Create a dictionary where keys are composite keys (FACILITY_ID, FACILITY_NAME, FACILITY_ADDRESS) and
                # values are (LATITUDE, LONGITUDE) tuples
                location_dict = {f"{row['VENDOR']}_{row['ADDRESS']}": (row['geometry'].y, row['geometry'].x)
                                 for _, row in previous_gdf.iterrows()}

        # Transform DataFrame to GeoDataFrame
        geometry = [Point(xy) for xy in zip(
            df.LONGITUDE, df.LATITUDE)]
        df = df.drop(['LONGITUDE', 'LATITUDE'], axis=1)
        gdf = gpd.GeoDataFrame(df, geometry=geometry)

        # Save GeoDataFrame to GeoJSON
        gdf.to_file(
            f"WIC_Food_Retailers/Geocoded_geojson/wic_food_retailers_{update_date}.geojson", driver='GeoJSON')

        # Define the GeoJSON file path
        geojson_file = f"WIC_Food_Retailers/Geocoded_geojson/wic_food_retailers_{update_date}.geojson"

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

    else:
        logger.info("No Update")

except Exception as e:
    logger.error("Exception occurred", exc_info=True)
