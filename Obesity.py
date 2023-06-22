import requests
import pandas as pd


def get_data_from_api():
    url = 'https://chronicdata.cdc.gov/resource/cwsq-ngmh.json'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f'Error: {response.status_code}')
        return None


def write_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


def main():
    data = get_data_from_api()

    if data is not None:
        write_to_csv(data, 'output.csv')
        print('Data written to output.csv')
    else:
        print('No data to write')


if __name__ == "__main__":
    main()
