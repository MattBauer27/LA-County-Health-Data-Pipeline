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

load_dotenv()

# create logger with 'spam_application'
logger = logging.getLogger('run_log')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
now = datetime.datetime.now()
logging_date = now.strftime("%Y-%m-%d")
fh = logging.FileHandler(f'Restaurants/RunLog/run_log.csv')
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

    # URL to your ArcGIS Feature Layer
    url_insp = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_LOS_ANGELES_COUNTY_RESTAURANT_AND_MARKET_INSPECTIONS_/FeatureServer/0"

    # URL to your ArcGIS Feature Layer
    url_loc = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_COUNTY_OF_LOS_ANGELES_RESTAURANT_AND_MARKET_INVENTORY_/FeatureServer/0"

    # Access the feature layer
    layer_insp = FeatureLayer(url_insp)

    # Access the feature layer
    layer_loc = FeatureLayer(url_loc)

    # Fetch metadata about the layer
    metadata_insp = layer_insp.properties

    # Fetch metadata about the layer
    metadata_loc = layer_loc.properties

    # Extract last update date (in UNIX timestamp format)
    last_update_unix_insp = metadata_insp.editingInfo.lastEditDate

    # Extract last update date (in UNIX timestamp format)
    last_update_unix_loc = metadata_loc.editingInfo.lastEditDate

    # Convert UNIX timestamp to readable format
    last_update_date_insp = datetime.datetime.fromtimestamp(
        last_update_unix_insp / 1000).strftime('%Y-%m-%d')

    # Convert UNIX timestamp to readable format
    last_update_date_loc = datetime.datetime.fromtimestamp(
        last_update_unix_loc / 1000).strftime('%Y-%m-%d')

    # Check if file exists and get the last update date
    file_path_insp = 'Restaurants/Dates_txts/Inspections/last_update_inspections.txt'
    if os.path.isfile(file_path_insp):
        with open(file_path_insp, 'r') as f:
            last_recorded_dates_insp = [line.strip()
                                        for line in f.readlines()]  # get all lines
    else:
        last_recorded_dates_insp = []

    # Check if file exists and get the last update date
    file_path_loc = 'Restaurants/Dates_txts/Locations/last_update_locations.txt'
    if os.path.isfile(file_path_loc):
        with open(file_path_loc, 'r') as f_loc:
            last_recorded_dates_loc = [line.strip()
                                       for line in f_loc.readlines()]  # get all lines
    else:
        last_recorded_dates_loc = []

    # If the last update date is more recent than the most recent date in the file, prepend it to the file
    if not last_recorded_dates_insp or last_update_date_insp > last_recorded_dates_insp[0]:
        with open(file_path_insp, 'w') as f:
            f.write(last_update_date_insp + '\n')
            for recorded_date in last_recorded_dates_insp:
                f.write(recorded_date + '\n')

        # Define the REST API URL
        query_url_insp = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_LOS_ANGELES_COUNTY_RESTAURANT_AND_MARKET_INSPECTIONS_/FeatureServer/0/query"

        params_insp = {
            "where": "PE_DESCRIPTION LIKE 'RESTAURANT%'",
            "outFields": "*",
            "f": "json",
            "returnGeometry": "false",
            "resultOffset": 0,
            "resultRecordCount": 1000
        }

        records_insp = []
        while True:
            # Send the request
            response_insp = requests.get(query_url_insp, params=params_insp)

            # Ensure the request was successful
            response_insp.raise_for_status()

            # Decode the response
            data_insp = response_insp.json()

            # Extract the records
            for attributes in data_insp['features']:
                records_insp.append(attributes['attributes'])

            # Check if there are more records
            if len(data_insp['features']) < params_insp["resultRecordCount"]:
                break

            # Get the next chunk of records
            params_insp["resultOffset"] += params_insp["resultRecordCount"]

        # Convert records to a pandas DataFrame
        new_records_df_insp = pd.DataFrame(records_insp)
        new_records_df_insp = new_records_df_insp.fillna('None')

        # Convert the 'ACTIVITY_DATE' column to datetime
        new_records_df_insp['ACTIVITY_DATE'] = pd.to_datetime(
            new_records_df_insp['ACTIVITY_DATE'], unit='ms')

        new_records_df_insp.to_csv(
            f"Restaurants/Base_csvs/Inspections/restaurants_insp_{last_update_date_insp}.csv", index=False, encoding='utf-8')
        if last_recorded_dates_insp:
            old_records_df_insp = pd.read_csv(
                f"Restaurants/Base_csvs/Inspections/restaurants_insp_{last_recorded_dates_insp[0]}.csv", encoding='utf-8')
            old_records_df_insp['ACTIVITY_DATE'] = pd.to_datetime(
                old_records_df_insp['ACTIVITY_DATE'])
            old_records_df_insp = old_records_df_insp.fillna('None')

            # Convert all columns to string type
            old_records_df_insp = old_records_df_insp.astype(str)
            new_records_df_insp = new_records_df_insp.astype(str)

            # Dataframe for rows present in new_records_df but not in old_records_df
            df_added_insp = new_records_df_insp[~new_records_df_insp.apply(
                tuple, 1).isin(old_records_df_insp.apply(tuple, 1))]
            df_added_insp.to_csv(
                f"Restaurants/Added/Inspections/restaurants_insp_added_{last_update_date_insp}.csv", index=False, encoding='utf-8')

            # Dataframe for rows present in old_records_df but not in new_records_df
            df_dropped_insp = old_records_df_insp[~old_records_df_insp.apply(
                tuple, 1).isin(new_records_df_insp.apply(tuple, 1))]
            df_dropped_insp.to_csv(
                f"Restaurants/Dropped/Inspections/restaurants_insp_dropped_{last_update_date_insp}.csv", index=False, encoding='utf-8')

    # If the last update date is more recent than the most recent date in the file, prepend it to the file
    if not last_recorded_dates_loc or last_update_date_loc > last_recorded_dates_loc[0]:
        with open(file_path_loc, 'w') as f_loc:
            f_loc.write(last_update_date_loc + '\n')
            for recorded_date_loc in last_recorded_dates_loc:
                f_loc.write(recorded_date_loc + '\n')

        # Define the REST API URL
        query_url_loc = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Public_Health_COUNTY_OF_LOS_ANGELES_RESTAURANT_AND_MARKET_INVENTORY_/FeatureServer/0/query"

        params_loc = {
            "where": "PE_DESCRIPTION LIKE 'RESTAURANT%'",
            "outFields": "*",
            "f": "json",
            "returnGeometry": "false",
            "resultOffset": 0,
            "resultRecordCount": 1000
        }

        records_loc = []
        while True:
            # Send the request
            response_loc = requests.get(query_url_loc, params=params_loc)

            # Ensure the request was successful
            response_loc.raise_for_status()

            # Decode the response
            data_loc = response_loc.json()

            # Extract the records
            for attributes_loc in data_loc['features']:
                records_loc.append(attributes_loc['attributes'])

            # Check if there are more records
            if len(data_loc['features']) < params_loc["resultRecordCount"]:
                break

            # Get the next chunk of records
            params_loc["resultOffset"] += params_loc["resultRecordCount"]

        # Convert records to a pandas DataFrame
        new_records_df_loc = pd.DataFrame(records_loc)
        new_records_df_loc = new_records_df_loc.fillna('None')

        new_records_df_loc.to_csv(
            f"Restaurants/Base_csvs/Locations/restaurants_loc_{last_update_date_loc}.csv", index=False, encoding='utf-8')
        if last_recorded_dates_loc:
            old_records_df_loc = pd.read_csv(
                f"Restaurants/Base_csvs/Locations/restaurants_loc_{last_recorded_dates_loc[0]}.csv", encoding='utf-8')
            old_records_df_loc = old_records_df_loc.fillna('None')

            # Convert all columns to string type
            old_records_df_loc = old_records_df_loc.astype(str)
            new_records_df_loc = new_records_df_loc.astype(str)

            # Dataframe for rows present in new_records_df but not in old_records_df
            df_added_loc = new_records_df_loc[~new_records_df_loc.apply(
                tuple, 1).isin(old_records_df_loc.apply(tuple, 1))]
            df_added_loc.to_csv(
                f"Restaurants/Added/Locations/restaurants_loc_added_{last_update_date_loc}.csv", index=False, encoding='utf-8')

            # Dataframe for rows present in old_records_df but not in new_records_df
            df_dropped_loc = old_records_df_loc[~old_records_df_loc.apply(
                tuple, 1).isin(new_records_df_loc.apply(tuple, 1))]
            df_dropped_loc.to_csv(
                f"Restaurants/Dropped/Locations/restaurants_loc_dropped_{last_update_date_loc}.csv", index=False, encoding='utf-8')

    # If there is an update in either the inspections data or the locations data
    if (not last_recorded_dates_insp or last_update_date_insp > last_recorded_dates_insp[0]) or (not last_recorded_dates_loc or last_update_date_loc > last_recorded_dates_loc[0]):

        logger.info("Data Updated")

        # Sort the inspections data by 'ACTIVITY_DATE' and 'FACILITY_ID'
        new_records_df_insp = new_records_df_insp.sort_values(
            by=['FACILITY_ID', 'ACTIVITY_DATE'])

        # Drop duplicates and keep the last (most recent) record for each 'FACILITY_ID'
        most_recent_insp = new_records_df_insp.drop_duplicates(
            subset='FACILITY_ID', keep='last')

        # Merge the locations data with the most recent inspections data
        merged_df = new_records_df_loc.merge(
            most_recent_insp[['FACILITY_ID', 'PROGRAM_STATUS']], on='FACILITY_ID', how='left')

        # Fill NaNs with 'ACTIVE'
        merged_df['PROGRAM_STATUS'].fillna('ACTIVE', inplace=True)

        # Rename 'PROGRAM_STATUS' to 'STATUS'
        merged_df.rename(columns={'PROGRAM_STATUS': 'STATUS'}, inplace=True)

        # Replace 'ACTIVE' with 'Open' and 'INACTIVE' with 'Closed' in 'STATUS' column
        merged_df['STATUS'] = merged_df['STATUS'].map(
            {'ACTIVE': 'Open', 'INACTIVE': 'Closed'})

        # Move 'STATUS' column to second position
        cols = merged_df.columns.tolist()
        cols.insert(1, cols.pop(cols.index('STATUS')))
        merged_df = merged_df[cols]

        merged_df = merged_df.fillna('None')
        merged_df = merged_df.astype(str)

        # Determine which file had the most recent update
        update_date = max(last_update_date_insp, last_update_date_loc)

        # Check if file exists and get the last update date
        file_path_merged = 'Restaurants/Dates_txts/Combined/last_update_combined.txt'
        if os.path.isfile(file_path_merged):
            with open(file_path_merged, 'r') as f:
                last_recorded_dates_merged = [
                    line.strip() for line in f.readlines()]  # get all lines
        else:
            last_recorded_dates_merged = []

        # If the last update date is more recent than the most recent date in the file, prepend it to the file
        if not last_recorded_dates_merged or update_date > last_recorded_dates_merged[0]:
            with open(file_path_merged, 'w') as f:
                f.write(update_date + '\n')
                for recorded_date in last_recorded_dates_merged:
                    f.write(recorded_date + '\n')

            # Save the merged data to a CSV file
            merged_df.to_csv(
                f"Restaurants/Base_csvs/Combined/restaurants_com_{update_date}.csv", index=False, encoding='utf-8')

            # If there are previous merged records
            if last_recorded_dates_merged:
                old_records_df_merged = pd.read_csv(
                    f"Restaurants/Base_csvs/Combined/restaurants_com_{last_recorded_dates_merged[0]}.csv", encoding='utf-8')

                # Reshape data to long format
                old_records_long = old_records_df_merged.melt(
                    id_vars='FACILITY_ID')
                new_records_long = merged_df.melt(id_vars='FACILITY_ID')

                old_records_df_merged = old_records_df_merged.fillna('None')

                # Convert all columns to string type for comparison
                old_records_df_merged = old_records_df_merged.astype(str)
                merged_df = merged_df.astype(str)

                # Dataframe for rows present in new_records_df but not in old_records_df
                df_added_merged = merged_df[~merged_df.apply(
                    tuple, 1).isin(old_records_df_merged.apply(tuple, 1))]
                df_added_merged.to_csv(
                    f"Restaurants/Added/Combined/restaurants_com_added_{update_date}.csv", index=False, encoding='utf-8')

                # Dataframe for rows present in old_records_df but not in new_records_df
                df_dropped_merged = old_records_df_merged[~old_records_df_merged.apply(
                    tuple, 1).isin(merged_df.apply(tuple, 1))]
                df_dropped_merged.to_csv(
                    f"Restaurants/Dropped/Combined/restaurants_com_dropped_{update_date}.csv", index=False, encoding='utf-8')

                # Compute the number of newly opened and closed facilities
                opened_df = merged_df[~merged_df['FACILITY_ID'].isin(
                    old_records_df_merged['FACILITY_ID'])]
                opened_from_closed = merged_df[(merged_df['FACILITY_ID'].isin(old_records_df_merged['FACILITY_ID'])) & (
                    old_records_df_merged['STATUS'] == 'Closed') & (merged_df['STATUS'] == 'Open')]
                opened = len(opened_df) + len(opened_from_closed)

                closed_df = old_records_df_merged[~old_records_df_merged['FACILITY_ID'].isin(
                    merged_df['FACILITY_ID'])]
                closed_from_open = merged_df[(merged_df['FACILITY_ID'].isin(old_records_df_merged['FACILITY_ID'])) & (
                    old_records_df_merged['STATUS'] == 'Open') & (merged_df['STATUS'] == 'Closed')]
                closed = len(closed_df) + len(closed_from_open)

                # Create new DataFrame to store the result
                result_df = pd.DataFrame(
                    {"Date": [update_date], "Opened": [opened], "Closed": [closed]})

                # Define CSV file
                csv_file = "Restaurants/Status_update/Status_updates.csv"

                # If the CSV exists, load it and append the new data
                if os.path.isfile(csv_file):
                    df = pd.read_csv(csv_file)
                    df = pd.concat([df, result_df])
                else:
                    df = result_df

                # Save to CSV
                df.to_csv(csv_file, index=False)

            # Find the feature layer to update
            feature_layer_name = "Restaurants_Inspection"
            search_result = gis_insp.content.search(
                query=f"title:\"{feature_layer_name}\" AND owner:{username}", item_type="Feature Service")

            # Identify the CSV file to use
            df = pd.read_csv(
                f"Restaurants/Base_csvs/Combined/restaurants_com_{update_date}.csv")

            df['FACILITY_ADDRESS'] = df['FACILITY_ADDRESS'].astype(str)
            df['FACILITY_CITY'] = df['FACILITY_CITY'].astype(str)
            df['FACILITY__STATE'] = df['FACILITY__STATE'].astype(str)
            df['FACILITY_ZIP'] = df['FACILITY_ZIP'].astype(str)
            df['FULL_ADDRESS'] = df['FACILITY_ADDRESS'] + ', ' + df['FACILITY_CITY'] + \
                ', ' + df['FACILITY__STATE'] + ', ' + df['FACILITY_ZIP']

            location_dict = {}
            # Load the existing GeoJSON file
            if last_recorded_dates_merged:
                previous_geojson_file = f"Restaurants/Geocoded_geojson/Geocoded_Combined_{last_recorded_dates_merged[0]}.geojson"
                if os.path.isfile(previous_geojson_file):
                    previous_gdf = gpd.read_file(previous_geojson_file)

                    # Create a dictionary where keys are composite keys (FACILITY_ID, FACILITY_NAME, FACILITY_ADDRESS) and
                    # values are (FACILITY_LATITUDE, FACILITY_LONGITUDE) tuples
                    location_dict = {f"{row['FACILITY_ID']}_{row['FACILITY_NAME']}_{row['FACILITY_ADDRESS']}": (row['geometry'].y, row['geometry'].x)
                                     for _, row in previous_gdf.iterrows()}

            # Geocoding addresses
            for index, row in df.iterrows():
                composite_key = f"{row['FACILITY_ID']}_{row['FACILITY_NAME']}_{row['FACILITY_ADDRESS']}"
                if composite_key in location_dict:
                    # If the composite_key exists in the location_dict, use the existing latitude and longitude
                    latitude, longitude = location_dict[composite_key]
                else:
                    try:
                        # If the composite_key doesn't exist in the location_dict, geocode the address
                        geocoded_data = geocode(row['FULL_ADDRESS'])[0]
                        latitude, longitude = geocoded_data['location']['y'], geocoded_data['location']['x']
                    except IndexError:
                        print(
                            f"Could not geocode address: {row['FULL_ADDRESS']}")
                        latitude, longitude = None, None

                df.at[index, 'FACILITY_LATITUDE'] = latitude
                df.at[index, 'FACILITY_LONGITUDE'] = longitude

            df = df.drop(columns=['FULL_ADDRESS'])

            # Transform DataFrame to GeoDataFrame
            geometry = [Point(xy) for xy in zip(
                df.FACILITY_LONGITUDE, df.FACILITY_LATITUDE)]
            df = df.drop(['FACILITY_LONGITUDE', 'FACILITY_LATITUDE'], axis=1)
            gdf = gpd.GeoDataFrame(df, geometry=geometry)

            # Save GeoDataFrame to GeoJSON
            gdf.to_file(
                f"Restaurants/Geocoded_geojson/Geocoded_Combined_{update_date}.geojson", driver='GeoJSON')

            # Define the GeoJSON file path
            geojson_file = f"Restaurants/Geocoded_geojson/Geocoded_Combined_{update_date}.geojson"

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
