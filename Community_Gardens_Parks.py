import requests
import subprocess

# specify the directory
directory = 'C:/Users/bauer/Google Drive/USC/Work/Research/DataPipeline/'

# Parks and gardens URL
parks_gardens_url = "https://www.google.com/maps/d/u/0/kml?mid=1gAIltWAx3egz2rjqB4cd3bL6I0yWOwM9&forcekml=1"

# Download the file with 'requests'
parks_gardens_response = requests.get(parks_gardens_url)

# Save the file
with open(directory + 'parksandgardens.kml', 'wb') as f:
    f.write(parks_gardens_response.content)

# Gardens URL
gardens_url = "https://www.google.com/maps/d/u/0/kml?mid=1HXMaJgriQuBFj8_qxpXZLn0Zrhf9-7KE&forcekml=1"

# Download the file with 'requests'
gardens_response = requests.get(gardens_url)

# Save the file
with open(directory + 'gardens.kml', 'wb') as f:
    f.write(gardens_response.content)

# paths to input KML and output shapefile
parks_gardens_kml = directory + "parksandgardens.kml"
parks_gardens_shapefile = directory + "parksandgardens.shp"

# construct the ogr2ogr command
parks_gardens_command = f'ogr2ogr -f "ESRI Shapefile" {parks_gardens_shapefile} {parks_gardens_kml}'

# execute the command
subprocess.run(parks_gardens_command, shell=True)

# paths to input KML and output shapefile
gardens_kml = directory + "gardens.kml"
gardens_shapefile = directory + "gardens.shp"

# construct the ogr2ogr command
gardens_command = f'ogr2ogr -f "ESRI Shapefile" {gardens_shapefile} {gardens_kml}'

# execute the command
subprocess.run(gardens_command, shell=True)

# execute the command and capture output and errors
parks_gardens_result = subprocess.run(
    parks_gardens_command, shell=True, capture_output=True, text=True)

# print output and errors
print("Parks and Gardens stdout:\n", parks_gardens_result.stdout)
print("Parks and Gardens stderr:\n", parks_gardens_result.stderr)

# execute the command and capture output and errors
gardens_result = subprocess.run(
    gardens_command, shell=True, capture_output=True, text=True)

# print output and errors
print("Gardens stdout:\n", gardens_result.stdout)
print("Gardens stderr:\n", gardens_result.stderr)
