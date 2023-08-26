# api_handler.py
import requests
import json
import pandas as pd
import pprint
from datetime import datetime, timedelta
from config import API_KEY,PARQUET_FILE_PATH
import time

def get_data_from_api(start_date, end_date, station_id):
    """
    Function to retrieve data from the AEMET API for a given date range and station id.
    """
    headers = {
        'accept': "application/json",
        'api_key': API_KEY
    }

    all_data = []

    # Calculate the number of 5-year intervals in the date range
    total_years = (end_date - start_date).days // 365
    intervals = total_years // 5

    # Iterate over the 5-year intervals
    for i in range(intervals + 1):
        # Calculate the start and end dates for the current interval
        interval_start_date = start_date + timedelta(days=i * 5 * 365)
        interval_end_date = min(interval_start_date + timedelta(days=5 * 365 - 1), end_date)

        #https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/2017-07-30T00:00:00UTC/fechafin/2023-08-31T23:59:59UTC/estacion/5530E/?api_key=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnZHV6YWptZXZoc3dwZWdqeWhAbml3Z2h4LmNvbSIsImp0aSI6IjFiNTIwYjUzLWRlNTUtNDMyYy1hMTQ3LWM0MzgyZGMyNzkwNCIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNjEwMTA5MzQxLCJ1c2VySWQiOiIxYjUyMGI1My1kZTU1LTQzMmMtYTE0Ny1jNDM4MmRjMjc5MDQiLCJyb2xlIjoiIn0.I2glcI451aMK2wvX9rVdOueFAVAcdxU58LSTQY1U8U4
        url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{interval_start_date.strftime('%Y-%m-%d')}T00:00:00UTC/fechafin/{interval_end_date.strftime('%Y-%m-%d')}T23:59:59UTC/estacion/{station_id}/?api_key={API_KEY}"        # Prepare the URL for the API request
        print(url)
        # Make the API request
        response = requests.request("GET", url, headers=headers)
        # Print the API response
        print(response.text)

        # If the request was successful, add the data to the list
        if response.status_code == 200:
            data = json.loads(response.text)

            # Make a second API request using the URL in the 'datos' field
            datos_url = data['datos']
            datos_response = requests.request("GET", datos_url, headers=headers)

            if datos_response.status_code == 200:
                datos_data = json.loads(datos_response.text)
                all_data.extend(datos_data)  # Use extend instead of append
            else:
                print(f"Error in second request: {datos_response.status_code}")
        else:
            print(f"Error in first request: {response.status_code}")

    return all_data


def get_all_stations():
    """
    Function to retrieve all stations from the AEMET API.
    """
    # Prepare the headers for the API request
    headers = {
        'accept': "application/json",
        'api_key': API_KEY
    }

    # Prepare the URL for the API request
    url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/"

    # Make the API request
    response = requests.request("GET", url, headers=headers)

    # If the request was successful, get the data from the 'datos' field
    if response.status_code == 200:
        data = json.loads(response.text)

        # Make a second API request using the URL in the 'datos' field
        datos_url = data['datos']
        datos_response = requests.request("GET", datos_url, headers=headers)

        if datos_response.status_code == 200:
            datos_data = json.loads(datos_response.text)
            return datos_data
        else:
            print(f"Error in second request: {datos_response.status_code}")
            return None
    else:
        print(f"Error in first request: {response.status_code}")
        return None    
    

def save_data_as_parquet(start_date, end_date, station_id, file_path=PARQUET_FILE_PATH):
    """
    Function to retrieve data from the AEMET API and save them as a Parquet file.
    """
    # Get the data
    data = get_data_from_api(start_date, end_date, station_id)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)

    # Save the DataFrame as a Parquet file
    df.to_parquet(f'{file_path}/data_{station_id}_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.parquet')


def save_stations_as_parquet(file_path=PARQUET_FILE_PATH):
    """
    Function to retrieve all stations from the AEMET API and save them as a Parquet file.
    """
    # Get all stations
    stations = get_all_stations()

    # Convert the data to a DataFrame
    df = pd.DataFrame(stations)

    # Save the DataFrame as a Parquet file
    df.to_parquet(f'{file_path}/stations.parquet')


def get_latest_data(start_date, end_date):
    """
    Function to retrieve the latest data from the AEMET API.
    """
    # Get the data for the date range
    data = get_data_from_api(start_date, end_date)

    # If no data was found for the current date, get the data for the oldest date
    if not data:
        print("No data found for today. Retrieving data for the oldest date...")
        oldest_date = end_date.replace(year=end_date.year - 1)
        data = get_data_from_api(oldest_date, end_date)

    return data