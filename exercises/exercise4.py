import urllib.request
import zipfile
import os
import pandas as pd
import sqlite3

# Download and Unzip Data
zip_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_file_path = "mowesta-dataset-20221107.zip"
urllib.request.urlretrieve(zip_url, zip_file_path)

# Extract data.csv from the ZIP file
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extract("data.csv", path=".")

# Delete the ZIP file
os.remove(zip_file_path)

# Reshape Data
data_path = "data.csv"
# Use the 'error_bad_lines=False' parameter to skip lines with extra fields
df = pd.read_csv("data.csv", sep=";", decimal=",", index_col=False,usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)","Batterietemperatur in °C", "Geraet aktiv"])

required_columns = [
    "Geraet",
    "Hersteller",
    "Model",
    "Monat",
    "Temperatur in °C (DWD)",
    "Batterietemperatur in °C",
    "Geraet aktiv"
]
df = df[required_columns]

column_names = {
    "Temperatur in °C (DWD)": "Temperatur",
    "Batterietemperatur in °C": "Batterietemperatur"
}
df = df.rename(columns=column_names)

# Transform Data
df["Temperatur"] = (df["Temperatur"] * 9 / 5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9 / 5) + 32

# Validate Data (Example: Geraet should be positive ID)
df = df[df["Geraet"] > 0]

# Write Data to SQLite Database
db_path = "temperatures.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

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

insert_query = "INSERT INTO temperatures VALUES (?, ?, ?, ?, ?, ?, ?)"

for row in df.itertuples(index=False):
    cursor.execute(insert_query, row)

conn.commit()
conn.close()