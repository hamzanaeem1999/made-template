import urllib.request as req
import zipfile as zip
import pandas as pd
import sqlite3 as db


# Extracting the data
def extraction_of_data(url):
    zip_filename = 'mowesta-dataset.zip'
    data_filename = 'data.csv'
    req.urlretrieve(url, zip_filename)
    with zip.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall()
    return data_filename

# Transforming the data
def transformation_of_data(filename):
    df = pd.read_csv(filename, sep=";", decimal=",", index_col=False,
                            usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
                                     "Batterietemperatur in °C", "Geraet aktiv"])

    # Conditions to be satisifed
    df = df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})
    selected_columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur", "Batterietemperatur", "Geraet aktiv"]
    df = df[selected_columns]
    df['Temperatur'] = (df['Temperatur'] * 9 / 5) + 32
    df['Batterietemperatur'] = (df['Batterietemperatur'] * 9 / 5) + 32

    return df

# Validation the data
def validation_of_data(df):
    df = (
        df[df['Geraet'] > 0]
        .loc[df['Hersteller'].astype(str).str.strip() != ""]
        .loc[df['Model'].astype(str).str.strip() != ""]
        .loc[df['Monat'].between(1, 12)]
        .loc[pd.to_numeric(df['Temperatur'], errors='coerce').notnull()]
        .loc[pd.to_numeric(df['Batterietemperatur'], errors='coerce').notnull()]
        .loc[df['Geraet aktiv'].isin(['Ja', 'Nein'])]
    )
    return df


def insert_in_database(df, database_name, table_name):
    conn = db.connect(database_name)
    cursor = conn.cursor()
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Geraet BIGINT, Hersteller TEXT, Model TEXT,Monat TEXT,
            Temperatur FLOAT,Batterietemperatur FLOAT,Geraet_aktiv TEXT)"""
    
    cursor.execute(create_table_query)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
data_filename = extraction_of_data(url)
transformed_data = transformation_of_data(data_filename)
validated_data = validation_of_data(transformed_data)
database_name = 'temperatures.sqlite'
table_name = 'temperatures'
insert_in_database(validated_data, database_name, table_name)
