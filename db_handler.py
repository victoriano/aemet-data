# db_handler.py
import duckdb
import pandas as pd
from config import DB_FILE_PATH

def save_data_to_db(df, db_file_path):
    """
    Function to save a pandas DataFrame to a DuckDB database.
    """
    # Initialize a DuckDB connection
    con = duckdb.connect(db_file_path)

    # Write the DataFrame to the database
    con.register('my_table', df)
    con.execute("CREATE TABLE IF NOT EXISTS weather_data AS SELECT * FROM my_table")

    # Close the connection
    con.close()

def load_data_from_db(db_file_path):
    """
    Function to load data from a DuckDB database into a pandas DataFrame.
    """
    # Initialize a DuckDB connection
    con = duckdb.connect(db_file_path)

    # Query the data from the database
    df = con.execute("SELECT * FROM weather_data").fetchdf()

    # Close the connection
    con.close()

    return df

def get_latest_date_from_db(db_file_path):
    """
    Function to get the latest date from the DuckDB database.
    """
    # Initialize a DuckDB connection
    con = duckdb.connect(db_file_path)

    # Query the latest date from the database
    latest_date = con.execute("SELECT MAX(date) AS latest_date FROM weather_data").fetchone()

    # Close the connection
    con.close()

    return latest_date
