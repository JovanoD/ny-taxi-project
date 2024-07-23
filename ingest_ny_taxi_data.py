import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    path = os.getcwd()

    # Windows Version
    # wsl_path = '/mnt/' + str.replace(path,'\\','/').replace(':','').lower()
    # os.system(f"""wsl ~ -e sh -c "cd '{wsl_path}' ; wget {url} -O {parquet_name}" """)

    # Linux Version
    os.system(f'wget {url} -O {parquet_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    parquet_file = pq.ParquetFile(parquet_name)
    i = 1
    total = 0
    for batch in parquet_file.iter_batches():
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        total += len(batch_df)
        print(f'Batch No. {i}, Total rows ingested = {total}')
        i += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest NY Taxi parquet data to postgres')

    parser.add_argument('--user', help='username for postgres database')
    parser.add_argument('--password', help='password for postgres credentials')
    parser.add_argument('--host', help='postgres database host')
    parser.add_argument('--port', type=int, help='port of a postgres database')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--table_name', help='the name of a table in database')
    parser.add_argument('--url', help='URL of the parquet')

    args = parser.parse_args()

    main(args)