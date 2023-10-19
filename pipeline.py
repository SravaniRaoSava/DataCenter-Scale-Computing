import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine

def extract_csv(source):
    return pd.read_csv(source)

def load_data(data, target):
    db_url = "postgresql+psycopg2://sravani:sravani123@db:5432/shelter"
    conn = create_engine(db_url)
    data.to_sql("outcomes", conn, if_exists="replace", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    args = parser.parse_args()
    print("Starting...")
    df = extract_csv(args.source)
    load_data(df, "outcomes")
    print("Completed")
