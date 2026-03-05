import pandas as pd
from load_cites_data import load_cites_data


OUTPUT_PATH = "data/processed/live_trade.csv"


def filter_live_trade(df):

    print("Filtering live animal trade...")

    live_df = df[df["Term"].str.upper() == "LIVE"]

    print("Records remaining:", len(live_df))

    return live_df


if __name__ == "__main__":

    cites_df = load_cites_data()

    live_trade = filter_live_trade(cites_df)

    print("Saving filtered dataset...")

    live_trade.to_csv(OUTPUT_PATH, index=False)

    print("Saved to:", OUTPUT_PATH)
    
