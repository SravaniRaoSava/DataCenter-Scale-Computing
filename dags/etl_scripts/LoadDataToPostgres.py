import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine

class GCSDataLoader:

    def __init__(self, bucket_name='datacenter11252023'):
        self.bucket_name = bucket_name

    def get_credentials(self):
        # Your service account credentials here
        credentials_info = {...}
        return credentials_info

    def connect_to_gcs_and_get_data(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        credentials_info = self.get_credentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCS into a DataFrame
        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcs_and_get_data(file_name)
        return df

class PostgresDataLoader:

    def __init__(self, db_config):
        self.db_config = db_config

    def get_queries(self, table_name):
        # Your table creation queries here
        if table_name == "dim_animals":
            query = """CREATE TABLE IF NOT EXISTS dim_animals (
                            ...
                        );
                        """
        elif table_name == "dim_outcome_types":
            query = """CREATE TABLE IF NOT EXISTS dim_outcome_types (
                            ...
                        );
                        """
        # Add similar queries for other tables

        else:
            query = """CREATE TABLE IF NOT EXISTS fct_outcomes (
                            ...
                        );
                        """
        return query

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()

        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcs_data)}")

def load_data_to_postgres_main(file_name, table_name):
    # Set your PostgreSQL database configuration here
    db_config = {
        'dbname': 'shelter',
        'user': 'sravani',
        'password': 'sravani123',
        'host': 'localhost',
        'port': '5432',
    }

    gcs_loader = GCSDataLoader()
    table_data_df = gcs_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader(db_config)
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
