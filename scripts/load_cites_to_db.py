import pandas as pd
import psycopg2
import os

DATA_FOLDER = "data/raw/cites_trade"

connection = psycopg2.connect(
    dbname="wildlife_trade",
    user="postgres",
    password="MYKL2011",
    host="localhost",
    port="5432"
)

cursor = connection.cursor()

files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

for file in files:

    path = os.path.join(DATA_FOLDER, file)

    print("Processing:", file)

    for chunk in pd.read_csv(path, chunksize=50000, low_memory=False):

        chunk = chunk.rename(columns={
            "Reporter.type": "reporter_type"
        })

        records = chunk[[
            "Id","Year","Appendix","Taxon","Class","Order","Family","Genus",
            "Term","Quantity","Unit","Importer","Exporter","Origin",
            "Purpose","Source","reporter_type"
        ]]

        for row in records.itertuples(index=False):

            cursor.execute("""
                INSERT INTO trade_records (
                    id, year, appendix, taxon, class, "order", family, genus,
                    term, quantity, unit, importer, exporter, origin,
                    purpose, source, reporter_type
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, row)

        connection.commit()

print("Finished loading all files")

cursor.close()
connection.close()
