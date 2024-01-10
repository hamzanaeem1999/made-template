import urllib.request
import zipfile
import os
import pandas as pd
import sqlite3

data_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_file_path = "mowesta_dataset.zip"

# Extraction of data
urllib.request.urlretrieve(data_url, zip_file_path)
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extract("data.csv", path=".")

# Select the desired number of columns
os.remove(zip_file_path)
selected_columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
                    "Batterietemperatur in °C", "Geraet aktiv"]

# Reading and Rename columns for clarity
df = pd.read_csv("data.csv", sep=";", decimal=",", index_col=False, usecols=selected_columns)

# Discard columns to the right of 'Geraet aktiv'
column_to_discard = "Geraet aktiv"
if column_to_discard in df.columns:
    discard_index = df.columns.get_loc(column_to_discard)
    df = df.iloc[:, :discard_index + 1]

# Rename columns for clarity
column_mapping = {
    "Temperatur in °C (DWD)": "Temperatur",
    "Batterietemperatur in °C": "Batterietemperatur"
}
df = df.rename(columns=column_mapping)
df["Temperatur"] = (df["Temperatur"] * 9 / 5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9 / 5) + 32
df = df[df["Geraet"] > 0]

# Write Data to SQLite Database
db_path = "temperatures.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table
create_table_query = """
CREATE TABLE temperatures (
    Geraet INTEGER,
    Hersteller TEXT,
    Model TEXT,
    Monat TEXT,
    Temperatur FLOAT,
    Batterietemperatur FLOAT,
    Geraet_aktiv TEXT
);
"""
cursor.execute(create_table_query)

# Insert data into the table
insert_query = "INSERT INTO temperatures VALUES (?, ?, ?, ?, ?, ?, ?)"
for row in df.itertuples(index=False):
    cursor.execute(insert_query, row)

# Commit changes and close connection
conn.commit()
conn.close()
