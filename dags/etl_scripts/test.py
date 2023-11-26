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
    
    headers = { 
        'accept': "application/json"
    }
    
    offset = 0
    all_data = []

    while offset < 157000:  # Assuming there are 157k records
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': order,
        }

        response = requests.get(base_url, headers=headers, params=params)
        current_data = response.json()
        
        # Break the loop if no more data is returned
        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data

def create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    data_list = []
    for entry in data:
        row_data = [entry.get(column, None) for column in columns]
        data_list.append(row_data)

    df = pd.DataFrame(data_list, columns=columns)
    return df

def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    credentials_info = {
        "type": "service_account",
        "project_id": "data-406304",
        "private_key_id": "03120f8a0cf96f1680c3610cf6c409da1090c10d",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCh0JdSxf4RY5u8\nM+WjFIgkXyQTyS0fbir9jntb81gDrxgVWRGOTeBYqv/FRlIm1CnGWAO/YFVQoGYy\nSEC6oClqRN0i5wCI3ybgsAlayDHLLZJjWi5aWcQXx1eC3YvlW8w/B4Vj/FQ2Fxvx\n7iqq370sYkosTCKGq2C4EE4wSgJy9Lurlvyrn4S8Ox46MKJ4FlqwVlw7GAJTz2ab\nWP6i4Y6Xbvx+cxwetyLyp3UhGA4GdPZM+XaYF/p9tUTrwRNRNXt2PhpYWpBYX8j3\nBocnXXzImPwnidrCDubM4qcHNvUi6KrLqm6rjMF+dE/CW597/lkVa6NfsipXPs/0\nxEDO7fIrAgMBAAECggEAIViMjqS5nljCE684L/q/sYsfsqofD96Somz10/WaWiv/\nkQqpWQOIa41VpxA+Rr0rHLIvl4UhH7vmQ49rM2plDn7BeXtUO2CNMPll/BzMUhwC\nklHXwovEIshUKuY/+ZSIyyZgIUIzEnCBtEIJ7eusntRkh6tq5Ai2JkhJ7J6b0Zd0\nENEbOOuod8jiQanVzGpsu3zgymRi2ahLn/A7nnBDUE+nAiqeAZkO7SUBeNfgBLnj\n/3JapjaLfXu+tujlC+ABdDLss90hAZSSJGoYJAR005s+Mo9djowg06SsqUTf5MWO\nvLzLuVnWheL8fcH5BWxrytX3bzzcaB4ErRSYFJ0ViQKBgQDkd9ebGs+xeLsbWSl/\nicpAPnYpoD9XWVvyxvs8za1O8RwE7T4GWHQNkTn3I2WPgCNskLlWfuCtnmln8OZS\nwltsdd7gquCSHYaqguCw6QUjKpVGUJy3V1JiiGV6pOJ8/qU20xYLiB8fQQ6C+I3v\ncj6fmqm5JkcB0HGX9bJhccNOIwKBgQC1UIS0VU3Z7XIxLSGlZR+aYoeHfaR4VD+p\nURqY//9vPrJJcqPAgA9h9LYRzoyJ+zBS/uZhRlqvlMT0e0FWuc2qRPkppUDOvjdI\nzMwGold2TggS4CgFRTEo/i8XSbmyWbVuQohq8fagK2FqBHOgQoLwwfLOCx/RZjM2\nGVYvJIGYWQKBgGVR4IQgt8r0x8WxHP46lT84dB6xumV6c5SPOgwVCq7frpVgwQj/\nYThDF3nKcNfa89rJs+fwDKPyhLxb0UkSeIj5HQZ1wXILNhWYRR0vud+Gmvy780Q7\nrYWVB6wEQ407QPm3Uyd6DpNFvrHGmOt/ttYBOROrLgJX9oh1pc4hLYQRAoGAc36Q\nqZZU+uaHgM+wyPw7P0pX7nSYGZYA30esW51mBmS3iJWfvXVVVx5cA8fAOfxpcdSw\nx3HbPlDTjLAelLHSUg2RcXe0an08FgFuTSOH9vIJroxescy7XdNcB8eMpBEEwvFa\nmFT8tlYrvS6MwJ7dbaL9mUX4xJgHMwS8o1brvjECgYAI0sgxwWdCCNQk5RZV6xNb\nd/j8y7RuQtJBs7hT6/M/4OO7RnEMXicdPRg+qqK7krmOauBNPBICbNqZ2tG1IE3n\ndF9e8AG4Y//s1lryqwZ9wvVA+VS/97usW4Fwgxfczy7zRlZI/u2U56ETSUC7W1kB\n7zG2MrtYQ4FGYNriiIdG7w==\n-----END PRIVATE KEY-----\n",
        "client_email": "dataproject@data-406304.iam.gserviceaccount.com",
        "client_id": "115373669330768095801",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dataproject%40data-406304.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS with date: {current_date}.")

def main1():
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
    main1()

