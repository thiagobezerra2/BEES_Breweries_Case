import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def aggregate_data():
    data_files = [f'data/silver/{f}' for f in os.listdir('data/silver') if f.endswith('.parquet')]
    df= pd.concat([pd.read_parquet(file) for file in data_files])

    aggregated_df = df.groupby(['state', 'brewery_type']).size().reset_index(name='count')

    os.makedirs('data/gold', exist_ok=True)
    table = pa.Table.from_pandas(aggregated_df)
    pq.write_table(table, 'data/gold/brewery_aggrefated_data.parquet')

if __name__ == '__main__':
    aggregate_data()