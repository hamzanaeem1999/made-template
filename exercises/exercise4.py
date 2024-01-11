import urllib.request as req
import zipfile as zip
import sqlite3 as db
import pandas as pd


def dataExtraction(url):
    zip_filename = 'mowesta-dataset.zip'
    data_filename = 'data.csv'
    req.urlretrieve(url, zip_filename)
    with zip.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall()
    return data_filename


def dataTransformer(filename):
    dataFrame = pd.read_csv(filename, sep=";", decimal=",", index_col=False,
                            usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
                                     "Batterietemperatur in °C", "Geraet aktiv"])

    # Rename columns
    dataFrame = dataFrame.rename(
        columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})

    # Discard columns
    columns_to_keep = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur", "Batterietemperatur", "Geraet aktiv"]
    dataFrame = dataFrame[columns_to_keep]

    # Celsius to Fahrenheit
    dataFrame['Temperatur'] = (dataFrame['Temperatur'] * 9 / 5) + 32

    #  Celsius to Fahrenheit
    dataFrame['Batterietemperatur'] = (dataFrame['Batterietemperatur'] * 9 / 5) + 32

    return dataFrame


def dataValidater(dataFrame):
    dataFrame = dataFrame[dataFrame['Geraet'] > 0]
    dataFrame = dataFrame[dataFrame['Hersteller'].astype(str).str.strip().ne("")]
    dataFrame = dataFrame[dataFrame['Model'].astype(str).str.strip().ne("")]
    dataFrame = dataFrame[dataFrame['Monat'].between(1, 12)]
    dataFrame = dataFrame[pd.to_numeric(dataFrame['Temperatur'], errors='coerce').notnull()]
    dataFrame = dataFrame[pd.to_numeric(dataFrame['Batterietemperatur'], errors='coerce').notnull()]
    dataFrame = dataFrame[dataFrame['Geraet aktiv'].isin(['Ja', 'Nein'])]
    return dataFrame


def saveToDB(dataFrame, database_name, table_name):
    conn = db.connect(database_name)
    cursor = conn.cursor()
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Geraet BIGINT,
            Hersteller TEXT,
            Model TEXT,
            Monat TEXT,
            Temperatur FLOAT,
            Batterietemperatur FLOAT,
            Geraet_aktiv TEXT
        )
    """
    cursor.execute(create_table_query)
    dataFrame.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()


# Driver
url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
data_filename = dataExtraction(url)
transformed_data = dataTransformer(data_filename)
validated_data = dataValidater(transformed_data)
database_name = 'temperatures.sqlite'
table_name = 'temperatures'
saveToDB(validated_data, database_name, table_name)
print("Done")