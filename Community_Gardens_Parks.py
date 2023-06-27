import requests
import subprocess
import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
from geopandas.tools import sjoin
import os
import shutil

# Parks and gardens URL
parks_gardens_url = "https://www.google.com/maps/d/u/0/kml?mid=1gAIltWAx3egz2rjqB4cd3bL6I0yWOwM9&forcekml=1"

# Download the file with 'requests'
parks_gardens_response = requests.get(parks_gardens_url)

# Save the file
with open('parksandgardens.kml', 'wb') as f:
    f.write(parks_gardens_response.content)

# Gardens URL
gardens_url = "https://www.google.com/maps/d/u/0/kml?mid=1HXMaJgriQuBFj8_qxpXZLn0Zrhf9-7KE&forcekml=1"

# Download the file with 'requests'
gardens_response = requests.get(gardens_url)

# Save the file
with open('gardens.kml', 'wb') as f:
    f.write(gardens_response.content)

# paths to input KML and output shapefile
parks_gardens_kml = "parksandgardens.kml"
parks_gardens_shapefile = "parksandgardens.shp"

# construct the ogr2ogr command
parks_gardens_command = f'ogr2ogr -f "ESRI Shapefile" {parks_gardens_shapefile} {parks_gardens_kml}'

# execute the command
subprocess.run(parks_gardens_command, shell=True)

# paths to input KML and output shapefile
gardens_kml = "gardens.kml"
gardens_shapefile = "gardens.shp"

# construct the ogr2ogr command
gardens_command = f'ogr2ogr -f "ESRI Shapefile" {gardens_shapefile} {gardens_kml}'

# execute the command
subprocess.run(gardens_command, shell=True)

# execute the command and capture output and errors
parks_gardens_result = subprocess.run(
    parks_gardens_command, shell=True, capture_output=True, text=True)

# Load the shapefiles into GeoDataFrames
both_gdf = gpd.read_file("parksandgardens.shp/both.shp")
gardens_gdf = gpd.read_file("parksandgardens.shp/gardens.shp")
parks_gdf = gpd.read_file("parksandgardens.shp/parks.shp")
communitygardens_gdf = gpd.read_file(
    "gardens.shp/Community Gardens in L.A. County.shp")
LACGC_gdf = gpd.read_file("gardens.shp/LACGC Community Gardens.shp")

# Concatenate the geodataframes
combined_gdf = pd.concat([both_gdf, gardens_gdf, parks_gdf,
                         communitygardens_gdf, LACGC_gdf], ignore_index=True)

# Remove duplicates based on the 'Name' field and geometry
combined_gdf = combined_gdf.drop_duplicates(subset=['Name', 'geometry'])

# Save the original CRS
original_crs = combined_gdf.crs

# Remove duplicates based on the 'Name' field and geometry
combined_gdf = combined_gdf.drop_duplicates(subset=['Name', 'geometry'])

# Project the data to UTM zone 11N (appropriate for Los Angeles)
combined_gdf = combined_gdf.to_crs("EPSG:26911")

# Create a buffer of 10 feet around each point
buffer = combined_gdf.buffer(20)

# Dissolve all geometries into one
all_areas = unary_union(buffer)

# Convert the unary_union result to a list of geometries
all_areas = [geom for geom in all_areas]

# Create a new GeoDataFrame from the list of geometries, setting the original CRS
buffer_gdf = gpd.GeoDataFrame(gpd.GeoSeries(all_areas), columns=[
                              'geometry'], crs="EPSG:26911")

# Create unique IDs for the new GeoDataFrame
buffer_gdf['ID'] = range(len(buffer_gdf))

# Spatially join the buffers with the original GeoDataFrame
joined = gpd.sjoin(combined_gdf, buffer_gdf, how='left', op='within')

# Keep only the first point in each group of points within each buffer area
unique_points = joined.sort_values('ID').groupby('ID').first()

# Set the CRS for the 'unique_points' DataFrame
unique_points.set_crs("EPSG:26911", inplace=True)

# Reproject back to the original CRS
unique_points = unique_points.to_crs(original_crs)

# Save the result as a new shapefile
unique_points.to_file("unique_points.shp")

shutil.rmtree("gardens.shp")
shutil.rmtree("parksandgardens.shp")
os.remove("gardens.kml")
os.remove("parksandgardens.kml")
