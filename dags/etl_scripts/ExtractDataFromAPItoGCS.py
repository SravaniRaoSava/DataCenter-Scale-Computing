import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')

def extract_data_from_api(limit=50000, order='animal_id'):
    """
    Function to extract data from data.austintexas.gov API.
    """
    base_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    headers = {'accept': "application/json"}
    offset = 0
    all_data = []

    while offset < 157000:  # Assuming there are 157k records
        params = {'$limit': str(limit), '$offset': str(offset), '$order': order}
        response = requests.get(base_url, headers=headers, params=params)
        current_data = response.json()

        # Break the loop if no more data is returned
        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data

def create_dataframe(data):
    """
    Create a DataFrame from the extracted data.
    """
    columns = ['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
               'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
               'breed', 'color']

    data_list = [[entry.get(column, None) for column in columns] for entry in data]
    df = pd.DataFrame(data_list, columns=columns)
    return df

def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    credentials_info = {
        # Your service account credentials here
    }

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)

    bucket = client.get_bucket(bucket_name)

    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)

    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS with date: {current_date}.")

def main():
    try:
        extracted_data = extract_data_from_api(limit=50000, order='animal_id')
        shelter_data = create_dataframe(extracted_data)

        gcs_bucket_name = 'datacenter11252023'
        gcs_file_path = 'data/{}/outcomes_{}.csv'

        upload_to_gcs(shelter_data, gcs_bucket_name, gcs_file_path)
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
