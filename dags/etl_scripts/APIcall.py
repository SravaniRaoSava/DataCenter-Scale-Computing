import requests
import pandas as pd
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def authenticate_google_drive():
    """
    Authenticate with Google Drive and return a PyDrive client.
    """
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and automatically handles authentication.
    drive = GoogleDrive(gauth)
    return drive

def upload_to_google_drive(api_url, folder_id):
    """
    Download data from an API, save it to a CSV file, and upload it to Google Drive.

    Parameters:
    - api_url (str): The URL of the API.
    - folder_id (str): The ID of the Google Drive folder to upload the file to.
    """
    # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Convert the JSON response to a DataFrame
        data = response.json()
        df = pd.DataFrame(data)

        # Add a timestamp column
        df['timestamp'] = datetime.now()

        # Save the DataFrame to a temporary CSV file
        temp_csv = "/path/to/temporary/file.csv"
        df.to_csv(temp_csv, index=False)

        # Authenticate with Google Drive
        drive = authenticate_google_drive()

        # Upload the file to Google Drive
        upload_file_to_drive(drive, temp_csv, folder_id)

        print(f"Data downloaded, saved to temporary file, and uploaded to Google Drive.")
    else:
        print(f"Failed to download data. Status code: {response.status_code}")

def upload_file_to_drive(drive, file_path, folder_id):
    """
    Upload a file to Google Drive.

    Parameters:
    - drive: PyDrive client.
    - file_path (str): Path to the file to be uploaded.
    - folder_id (str): ID of the Google Drive folder to upload the file to.
    """
    file_drive = drive.CreateFile({'title': file_path.split("/")[-1], 'parents': [{'id': folder_id}]})
    file_drive.Upload()

# Example usage:
api_url = "https://data.austintexas.gov/resource/9t4d-g238.json"
folder_id = "your_google_drive_folder_id"

upload_to_google_drive(api_url, folder_id)
