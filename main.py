# main.py

from data_handler import handle_data, get_data_for_date
from db_handler import get_latest_date_from_db
from config import DB_FILE_PATH, CHECK_DATE_FORMAT
from datetime import datetime, timedelta

def main():
    """
    Main function to handle the data retrieval and storage process.
    """
    # Get the latest date from the database
    latest_date = get_latest_date_from_db(DB_FILE_PATH)

    # If a latest date was found, get the data for the next day
    if latest_date:
        next_date = datetime.strptime(latest_date, CHECK_DATE_FORMAT) + timedelta(days=1)
        get_data_for_date(next_date)
    # If no latest date was found, handle the data normally
    else:
        handle_data()

if __name__ == "__main__":
    main()