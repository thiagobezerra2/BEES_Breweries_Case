import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os 

#Transformation and partition data

def transform_data():

    df = pd.read_json('data/bronze/brewery_data.json')

    df = df.dropna(subset = ['id','name', 'state'])

#Remove blank space
    df['country'] = df['country'].str.replace(' ', '_')
    df['state'] = df['state'].str.replace(' ', '-')

#Save transformed data partitioned by state
    os.makedirs('data/silver', exist_ok=True)
    for state, group in df.groupby('state'):
        table = pa.Table.from_pandas(group)
        pq.write_table(table, f'data/silver/brewery_data_{state}.parquet')

if __name__ == '__main__':
    transform_data()