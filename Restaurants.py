from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

# Initialize the GIS
gis_insp = GIS("https://uscssi.maps.arcgis.com",
               client_id=os.getenv('client_id'))

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
last_update_date_insp = datetime.fromtimestamp(
    last_update_unix_insp / 1000).strftime('%Y-%m-%d')

# Convert UNIX timestamp to readable format
last_update_date_loc = datetime.fromtimestamp(
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
