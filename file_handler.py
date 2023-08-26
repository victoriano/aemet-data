# file_handler.py

import pandas as pd
from config import PARQUET_FILE_PATH

def save_data_to_parquet(df, file_path):
    """
    Function to save a pandas DataFrame to a parquet file.
    """
    # Check if the file already exists
    try:
        existing_df = pd.read_parquet(file_path)
        # If it does, append the new data
        df = pd.concat([existing_df, df])
    except FileNotFoundError:
        # If it doesn't, just write the new data
        pass

    # Save the DataFrame to a parquet file
    df.to_parquet(file_path)

def load_data_from_parquet(file_path):
    """
    Function to load data from a parquet file into a pandas DataFrame.
    """
    try:
        df = pd.read_parquet(file_path)
        return df
    except FileNotFoundError:
        print(f"No parquet file found at: {file_path}")
        return None
