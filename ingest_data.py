#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd # type: ignore
import pyarrow.parquet as pq # type: ignore
from sqlalchemy import create_engine # type: ignore
from time import time
import os, sys

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # get the name of the file from the url
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f"Downloading {file_name}...")
    
    # download the file from the url
    os.system(f"wget {url} -O {file_name}")
    print("\n")

    # SQL engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    
    if ".csv" in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif ".parquet" in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else:
        print("Error. Only .csv and .parquet file accepted.")
        sys.exit()

    # Create sql table
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    # Insert values to sql by batch
    t_start = time()
    count = 0
    for batch in df_iter:
        count+=1

        if ".parquet" in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        
        print(f"Inserting batch {count}...")

        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, if_exists="append")
        b_end = time()

        print(f"Inserted! Time taken: {b_end - b_start:10.3f} seconds.\n")
    
    t_end = time()
    print(f"Completed! Total time taken: {t_end-t_start:10.3f} seconds to complete {count} batches.")



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="IngestData",
        description="Ingest data to local postgres",
        epilog="see taxi-data-ingestion file",
    )

    # creating a dicitionary of keys for your main function, but it will have help details.
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of table where results goes in")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)