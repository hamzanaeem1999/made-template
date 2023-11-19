import numpy as np
import pandas as pd
from sqlalchemy import create_engine
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
    db_path = os.path.join(current_dir,"..","data", "munich.sqlite")
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as connection:
        df.to_sql(table, connection, if_exists="replace")
def main():
#   First CSV
    weather_data_munich = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=T86CZSABGBBZ3ELJMZ44JRUET&taskId=337723d75a8b19339b3bc507223c7cc6&zip=false"
#   Extract
    after_reading_weather_csv = extract(weather_data_munich)
    drop_weather_columns = ['name', 'snow', 'windgust','preciptype', 'precip','precipprob','severerisk', 'sunrise',
                    'sunset','moonphase','precipcover','description',"icon","stations","visibility"]
#   Transform
    refined_weather_data = transform(after_reading_weather_csv, drop_weather_columns, key="munich_weather")
#   Load
    load(refined_weather_data, "table_1")

#   Second CSV
    bike_traffic_data = "/home/hamza-developer/Videos/3rd_semester/Advance_Data_Engineering/archive/rad_15min.csv"
#   Extract
    after_reading_bike_traffic_csv = extract(bike_traffic_data)   
#   Transform
    drop_bike_columns = ['richtung_1','uhrzeit_ende','uhrzeit_start','richtung_2']
    refined_bike_traffic_data = transform(after_reading_bike_traffic_csv, drop_bike_columns,  key="bike_traffic")
#   Load
    load(refined_bike_traffic_data, "table_2")
if __name__ == "__main__":
    main()

