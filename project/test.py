import pandas as pd
import os
from sqlalchemy import create_engine, inspect
from pipeline import extract, transform, download_kaggle_dataset

# Class to handle ETL processes for test case
class ETLHandler:
    def __init__(self):
        pass

    def perform_extraction(self, file_path):
        data = extract(file_path)
        if data.empty:
            raise ValueError("Extraction failed: Error in Extraction !.")
        print("Data Extraction Test: Test Passed")
        return data

    def perform_transformation(self, data, columns_to_drop, key):
        transformed_data = transform(data, columns_to_drop, key)
        if transformed_data.isna().any().any():
            raise ValueError("Transformation failed: Error in Transformation !.")
        print("Data Transformation Test: Test Passed")
        return transformed_data

    def perform_loading_check(self, table_name):
        engine = create_engine("sqlite:///../data/munich.sqlite")
        if not inspect(engine).has_table(table_name):
            raise ValueError(f"Loading failed: Table '{table_name}' does not exist")
        print(f"Data Loading Test: Table '{table_name}' exists, Test Passed")
        return True

def main():
    etl = ETLHandler()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, ".", "data")

    # Process Weather Data
    weather_data_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=T86CZSABGBBZ3ELJMZ44JRUET&taskId=337723d75a8b19339b3bc507223c7cc6&zip=false"
    weather_data = etl.perform_extraction(weather_data_url)
    weather_columns_to_drop = ['name', 'snow', 'windgust', 'preciptype', 'precip', 'precipprob', 
                               'severerisk', 'sunrise', 'sunset', 'moonphase', 'precipcover', 
                               'description', "icon", "stations", "visibility"]
    etl.perform_transformation(weather_data, weather_columns_to_drop, "munich_weather")
    etl.perform_loading_check("table_1")

    # Process Bike Traffic Data
    kaggle_dataset = 'lucafrance/bike-traffic-in-munich'
    bike_data_filename = 'rad_15min.csv'
    download_kaggle_dataset(kaggle_dataset, data_dir, bike_data_filename)
    bike_data_path = os.path.join(data_dir, bike_data_filename)
    bike_data = etl.perform_extraction(bike_data_path)
    bike_columns_to_drop = ['richtung_1', 'uhrzeit_ende', 'uhrzeit_start', 'richtung_2']
    etl.perform_transformation(bike_data, bike_columns_to_drop, "bike_traffic")
    etl.perform_loading_check("table_2")

if __name__ == "__main__":
    main()
