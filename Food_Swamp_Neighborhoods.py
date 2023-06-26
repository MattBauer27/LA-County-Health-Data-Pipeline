import json
import urllib.request
import pandas as pd

url = 'https://data.chhs.ca.gov/api/3/action/datastore_search?resource_id=c58c1b68-6545-4039-bc52-6facf120a4c7&limit=100'
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

# Export the DataFrame to a CSV file
df1.to_csv('foodswamps.csv', index=False, encoding='utf-8-sig')
