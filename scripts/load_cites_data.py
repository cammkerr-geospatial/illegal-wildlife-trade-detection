import pandas as pd
import os

# folder containing the CITES CSV files
DATA_FOLDER = "data/raw/cites_trade"

def load_cites_data():

    print("Loading CITES trade data...")

    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

    dataframes = []

    for file in files:
        path = os.path.join(DATA_FOLDER, file)
        print(f"Reading {file}")

        df = pd.read_csv(path, low_memory=False)
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)

    print("Total records loaded:", len(combined_df))

    return combined_df


if __name__ == "__main__":
    cites_data = load_cites_data()

    print(cites_data.head())
    