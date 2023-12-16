from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi
import numpy as np
import pandas as pd
import zipfile
import os

def extract(get_csv):
    df = pd.read_csv(get_csv)
    return df

def transform(df, drop_columns, key):
    if key == "bike_traffic":
        df = df.drop(drop_columns, axis=1)
        df['datum'] = pd.to_datetime(df['datum'])
        df['date'] = df['datum'].dt.date
        # Filter data for the year 2020
        start_date = pd.Timestamp('2020-01-01')
        end_date = pd.Timestamp('2020-12-31')
        df_2020 = df[(df['date'] >= start_date.date()) & (df['date'] <= end_date.date())]
        # Group by date and sum the 'gesamt' column for each date, rename the column to 'bikes'
        result_df = df_2020.groupby('date')['gesamt'].sum().reset_index().rename(columns={'gesamt': 'bikes'})
        return result_df
    df = df.drop(drop_columns, axis=1)
    return df

def load(df, table):
    current_dir = os.getcwd()  # Get the current working directory
    db_path = os.path.join(current_dir,".","data", "munich.sqlite")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as connection:
        df.to_sql(table, connection, if_exists="replace")
      
    
def download_kaggle_dataset(dataset, target_folder, filename):
    api = KaggleApi()
    api.authenticate()
    username, dataset_name = dataset.split('/')[-2:]
    zip_file_path = os.path.join(target_folder, f"{dataset_name}.zip")
    api.dataset_download_files(f"{username}/{dataset_name}", path=target_folder, unzip=False)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extract(filename, path=target_folder)


def main():
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "..", "data")
    weather_data_munich = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=T86CZSABGBBZ3ELJMZ44JRUET&taskId=337723d75a8b19339b3bc507223c7cc6&zip=false"
    after_reading_weather_csv = extract(weather_data_munich)
    drop_weather_columns = ['name', 'snow', 'windgust','preciptype', 'precip','precipprob','severerisk', 'sunrise',
                    'sunset','moonphase','precipcover','description',"icon","stations","visibility"]
    refined_weather_data = transform(after_reading_weather_csv, drop_weather_columns, key="munich_weather")
    load(refined_weather_data, "table_1")

# To run the bike traffic data csv below, you need to follow below steps:
# 1) Create a Kaggle Account:
#       If you don't have a Kaggle account, you'll need to create one, also install Kaggle API.

# 2) Enable Kaggle API:
#      Log in to your Kaggle account.
#      Go to your account settings page.
#      Scroll down to the "API" section and click on "Create New API Token."
#      This will download a file named kaggle.json to your computer.
#      Store Kaggle API Key:

# 3) Place the downloaded kaggle.json file in a directory named as .kaggle on your machine.
    
    download_kaggle_dataset('lucafrance/bike-traffic-in-munich', data_dir, 'rad_15min.csv')
    bike_traffic_data = os.path.join(data_dir, 'rad_15min.csv')

    after_reading_bike_traffic_csv = extract(bike_traffic_data)  
    drop_bike_columns = ['richtung_1','uhrzeit_ende','uhrzeit_start','richtung_2']
    refined_bike_traffic_data = transform(after_reading_bike_traffic_csv, drop_bike_columns,  key="bike_traffic")
    load(refined_bike_traffic_data, "table_2")
if __name__ == "__main__":
    main()
# When you want to run pipeline.sh you just have to run like this command on linux: ./pipeline.sh in the same
# directory, if you don't do you then have to paste whole path to run this command
