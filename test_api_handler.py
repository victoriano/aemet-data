import unittest, pprint
import os
import pandas as pd
from config import PARQUET_FILE_PATH
from datetime import datetime
from api_handler import get_data_from_api, save_data_as_parquet, get_all_stations, save_stations_as_parquet

class TestAPIHandler(unittest.TestCase):
    def test_get_data_from_api(self):
        start_date = datetime(2015, 1, 1)
        end_date = datetime(2021, 12, 31)
        station_id = '3195'  
        data = get_data_from_api(start_date, end_date, station_id)
        
        # Print the first 5 records
        print("First 2 records:")
        pprint.pprint(data[:2])
        
        # Print the last 5 records
        print("Last 2 records:")
        pprint.pprint(data[-2:])
        
        self.assertIsInstance(data, list)
    
    def test_save_data_as_parquet(self):
        start_date = datetime(1930, 1, 1)
        end_date = datetime(2023, 12, 31)
        station_id = '3195'  
        save_data_as_parquet(start_date, end_date, station_id, PARQUET_FILE_PATH)
            
        # Check that the file was created
        self.assertTrue(os.path.exists(f'{PARQUET_FILE_PATH}/data_{station_id}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.parquet'))
            
        # Check that the file is not empty
        df = pd.read_parquet(f'{PARQUET_FILE_PATH}/data_{station_id}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.parquet')
        self.assertFalse(df.empty)

    def test_get_all_stations(self):
        api_key = 'your_api_key'  # replace with your actual API key
        stations = get_all_stations()
        
        # Print all stations
        print("All stations:")
        pprint.pprint(stations)
        
        self.assertIsInstance(stations, list)

    def test_save_stations_as_parquet(self):
        save_stations_as_parquet(PARQUET_FILE_PATH)
            
        # Check that the file was created
        self.assertTrue(os.path.exists(f'{PARQUET_FILE_PATH}/stations.parquet'))
            
        # Check that the file is not empty
        df = pd.read_parquet(f'{PARQUET_FILE_PATH}/stations.parquet')
        self.assertFalse(df.empty)


if __name__ == '__main__':
    unittest.main()