# data_handler.py
import pandas as pd
from api_handler import get_latest_data, get_data_from_api  # Add the missing function here
from file_handler import save_data_to_parquet
from db_handler import save_data_to_db
from config import PARQUET_FILE_PATH, DB_FILE_PATH, CHECK_DATE_FORMAT

def handle_data():
    """
    Function to handle the data retrieval and storage process.
    """
    # Get the latest data from the API
    data = get_latest_data()

    # If data was found, process it
    if data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the data to a parquet file
        save_data_to_parquet(df, PARQUET_FILE_PATH)

        # Save the data to the database
        save_data_to_db(df, DB_FILE_PATH)

    else:
        print("No data found.")

def get_data_for_date(date):
    """
    Function to retrieve data for a specific date and save it to a parquet file and the database.
    """
    # Convert the date to the required format
    date_str = date.strftime(CHECK_DATE_FORMAT)

    # Get the data for the date
    data = get_data_from_api(date_str)

    # If data was found, process it
    if data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the data to a parquet file
        save_data_to_parquet(df, PARQUET_FILE_PATH)

        # Save the data to the database
        save_data_to_db(df, DB_FILE_PATH)

    else:
        print(f"No data found for date: {date_str}")
