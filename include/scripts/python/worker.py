import pandas as pd 
import numpy as np
from datetime import datetime
from io import BytesIO
import pytz
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import os

register_adapter(np.int64, AsIs)

COL_type={
    'Category': 'category',
    'Sub-Category': 'category',
    'Month': 'str',
    'Millions of Dollars': 'int',
}

TMP_PATH = './include/resources/data/tmp/'

CATEGORY_MAPPING = {'alcoholic':'Alcoholic beverages', 
                    'cereals_bakery':'Cereals and bakery products', 
                    'meats_poultry':'Meats and poultry'}

def extract(file_path: str) -> str:
    df = pd.read_csv(file_path, dtype=COL_type)
    df = df[df['Category'].isin(CATEGORY_MAPPING.values())]
    return df.to_json()

def transform(data_json:str) -> list:
    df = pd.read_json(data_json)
    df.rename(
        columns= {
            'Category': 'category',
            'Sub-Category': 'sub_category',
            'Month': 'aggregation_date',
            'Millions of Dollars': 'millions_of_dollar'
        },
        inplace=True
    )
    df['aggregation_date'] = pd.to_datetime(df['aggregation_date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    df['pipeline_exc_datetime'] = datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')
    file_paths = []
    for table_name, category in CATEGORY_MAPPING.items():
        sub_df = df[df['category'] == category]
        path = f'{TMP_PATH}{table_name}.csv'
        sub_df.to_csv(path, index=False)
        file_paths.append(path)
    return file_paths

# def load(transformed_data_path: str, pg_hook):
#     table_name = f"consumption_{transformed_data_path.replace(TMP_PATH, '').replace('.csv', '')}_yyyymmdd"
#     target_columns = ['category', 'sub_category', 'aggregation_date', 'millions_of_dollar', 'pipeline_exc_datetime']
#     df = pd.read_csv(f"{transformed_data_path}")[target_columns]
#     df['millions_of_dollar'] = df['millions_of_dollar'].astype(int)
#     data_to_insert = [tuple(row) for row in \
#                       df.to_records(index=False)]
#     pg_hook.insert_rows(table=table_name, rows=data_to_insert, target_fields=target_columns, commit_every=1000)
#     os.remove(transformed_data_path)

def load(transformed_data_path: str, pg_hook):
    # Use the PostgresHook to get the connection details
    connection = pg_hook.get_connection(conn_id='postgres')
    # Define the database connection parameters
    db_params = {
        'database': connection.schema,  # or connection.extra_dejson['database'] if you use a custom parameter
        'user': connection.login,
        'password': connection.password,
        'host': connection.host,
        'port': connection.port
    }
    # Read a CSV file into a DataFrame
    df = pd.read_csv(transformed_data_path)
    # Convert the DataFrame to a binary CSV format in memory
    binary_csv_data = BytesIO()
    df.to_csv(binary_csv_data, index=False, encoding='utf-8')
    del df
    # Reset the stream position for reading
    binary_csv_data.seek(0)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    # Create a cursor
    cur = conn.cursor()
    # Define the table name where you want to copy the data
    table_name = table_name = f"consumption_{transformed_data_path.replace(TMP_PATH, '').replace('.csv', '')}_yyyymmdd"
    # Use the COPY command to copy data from the binary CSV data in memory to the database
    cur.copy_expert(sql=f"COPY {table_name} FROM stdin CSV HEADER", file=binary_csv_data)
    # Commit the changes
    conn.commit()
    # Close the cursor and connection
    cur.close()
    conn.close()
    os.remove(transformed_data_path)

def check_records(table_names: list):
    qurery = ''
    tables_count = len(table_names)
    for idx, table_name in enumerate(table_names):
        qurery += F"""SELECT '{table_name}', COUNT(*) FROM 
                        consumption_{table_name}_yyyymmdd"""
        
        if idx < tables_count - 1:
            qurery += '\nUNION ALL\n'
        else:
            qurery += ';'
    return qurery




    # copy_command = f'''
    #     COPY {table_name}(category, sub_category, aggregation_date, millions_of_dollar, pipeline_exc_datetime)
    #     FROM '{transformed_data_path.split('/')[-1]}'
    #     DELIMITER ','
    #     CSV HEADER
    # '''

#     copy_command = (
#     'psql -d postgres -U postgres -c "'
#     f'COPY {table_name}(category, sub_category, aggregation_date, millions_of_dollar, pipeline_exc_datetime) '
#     f"FROM '{transformed_data_path.split('/')[-1]}' "
#     "DELIMITER ',' "
#     'CSV HEADER"'
# )
    # subprocess.run(copy_command)

