import pandas as pd
from sqlalchemy import create_engine

# Get data csv file
def extract_csv(data_path):
    df = pd.read_csv(data_path, sep=';', low_memory=False)
    return df

# Transform the data under guidelines mentioned by professor in document
def transform_data(data):
    # 1) Drop the "Status" column
    data = data.drop('Status', axis=1)

    # Step 2: Drop rows with invalid values, can't change str to int so first replace these things.
    data['Laenge'] = data['Laenge'].str.replace(',', '.').astype(float)
    data['Breite'] = data['Breite'].str.replace(',', '.').astype(float)

    # 2) Data Validation
    data = data[
        (data["Verkehr"].isin(["FV", "RV", "nur DPN"])) &
        (data["Laenge"].between(-90, 90)) &
        (data["Breite"].between(-90, 90)) &
        (data["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))
        ].dropna()

    # 3) Change Data Types
    data_type = {"EVA_NR": int,"DS100": str,"IFOPT": str,"NAME": str,"Verkehr": str,"Laenge": float,
                "Breite": float,"Betreiber_Name": str,"Betreiber_Nr": int}
    data = data.astype(data_type)
    return data


# Loader
def load_data(transformed_data, table_name):
    engine = create_engine("sqlite:///trainstops.sqlite")
    transformed_data.to_sql(table_name, engine, if_exists="replace", index=False)


def main():
    data_path = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    after_reading_csv = extract_csv(data_path)
    after_transforming = transform_data(after_reading_csv)
    load_data(after_transforming, "trainstops")

if __name__ == "__main__":
    main()