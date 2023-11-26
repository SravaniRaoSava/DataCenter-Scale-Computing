import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

class DataTransformer:
    def __init__(self):
        self.mountain_time_zone = pytz.timezone('US/Mountain')
        self.outcomes_map = {'Rto-Adopt': 1, 'Adoption': 2, 'Euthanasia': 3, 'Transfer': 4,
                             'Return to Owner': 5, 'Died': 6, 'Disposal': 7, 'Missing': 8,
                             'Relocate': 9, 'N/A': 10, 'Stolen': 11}

    def get_credentials(self):
        bucket_name = "datacenter11252023"
        credentials_info = {
            "type": "service_account",
            "project_id": "data-406304",
            # ... (other credentials)
        }

        return credentials_info, bucket_name

    def connect_to_gcs_and_get_data(self, credentials_info, gcs_bucket_name):
        gcs_file_path = 'data/{}/outcomes_{}.csv'

        client = storage.Client.from_service_account_info(credentials_info)

        bucket = client.get_bucket(gcs_bucket_name)

        current_date = datetime.now(self.mountain_time_zone).strftime('%Y-%m-%d')
        formatted_file_path = gcs_file_path.format(current_date, current_date)

        blob = bucket.blob(formatted_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def write_data_to_gcs(self, dataframe, credentials_info, bucket_name, file_path):
        print(f"Writing data to GCS.....")

        client = storage.Client.from_service_account_info(credentials_info)
        csv_data = dataframe.to_csv(index=False)

        bucket = client.get_bucket(bucket_name)

        blob = bucket.blob(file_path)
        blob.upload_from_string(csv_data, content_type='text/csv')
        print(f"Finished writing data to GCS.")

    def prep_data(self, data):
        data['name'] = data['name'].str.replace("*", "", regex=False)

        data['sex'] = data['sex_upon_outcome'].replace({"Neutered Male": "M", "Intact Male": "M",
                                                        "Intact Female": "F", "Spayed Female": "F",
                                                        "Unknown": np.nan})

        data['is_fixed'] = data['sex_upon_outcome'].replace({"Neutered Male": True, "Intact Male": False,
                                                             "Intact Female": False, "Spayed Female": True,
                                                             "Unknown": np.nan})

        data['ts'] = pd.to_datetime(data.datetime)
        data['date_id'] = data.ts.dt.strftime('%Y%m%d')
        data['time'] = data.ts.dt.time

        data['outcome_type_id'] = data['outcome_type'].fillna("N/A")
        data['outcome_type_id'] = data['outcome_type'].replace(self.outcomes_map)

        return data

    def prep_animal_dim(self, data):
        print("Preparing Animal Dimensions Table Data")
        animal_dim = data[['animal_id', 'name', 'date_of_birth', 'sex', 'animal_type', 'breed', 'color']]
        animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

        mode_sex = animal_dim['sex'].mode().iloc[0]
        animal_dim['sex'] = animal_dim['sex'].fillna(mode_sex)

        return animal_dim.drop_duplicates()

    def prep_date_dim(self, data):
        print("Preparing Date Dimension Table Data")
        dates_dim = pd.DataFrame({
            'date_id': data.ts.dt.strftime('%Y%m%d'),
            'date': data.ts.dt.date,
            'year': data.ts.dt.year,
            'month': data.ts.dt.month,
            'day': data.ts.dt.day,
        })
        return dates_dim.drop_duplicates()

    def prep_outcome_types_dim(self, data):
        print("Preparing Outcome Types Dimension Table Data")
        outcome_types_dim = pd.DataFrame.from_dict(self.outcomes_map, orient='index').reset_index()
        outcome_types_dim.columns = ['outcome_type', 'outcome_type_id']
        return outcome_types_dim

    def prep_outcomes_fct(self, data):
        print("Preparing Outcome Fact Table Data")
        outcomes_fct = data[["animal_id", 'date_id', 'time', 'outcome_type_id', 'is_fixed']]
        return outcomes_fct

    def transform_data(self):
        credentials_info, bucket_name = self.get_credentials()

        new_data = self.connect_to_gcs_and_get_data(credentials_info, bucket_name)

        new_data = self.prep_data(new_data)

        dim_animal = self.prep_animal_dim(new_data)
        dim_dates = self.prep_date_dim(new_data)
        dim_outcome_types = self.prep_outcome_types_dim(new_data)
        fct_outcomes = self.prep_outcomes_fct(new_data)

        dim_animal_path = "transformed_data/dim_animal.csv"
        dim_dates_path = "transformed_data/dim_dates.csv"
        dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
        fct_outcomes_path = "transformed_data/fct_outcomes.csv"

        self.write_data_to_gcs(dim_animal, credentials_info, bucket_name, dim_animal_path)
        self.write_data_to_gcs(dim_dates, credentials_info, bucket_name, dim_dates_path)
        self.write_data_to_gcs(dim_outcome_types, credentials_info, bucket_name, dim_outcome_types_path)
        self.write_data_to_gcs(fct_outcomes, credentials_info, bucket_name, fct_outcomes_path)


# Example usage:
transformer = DataTransformer()
transformer.transform_data()
