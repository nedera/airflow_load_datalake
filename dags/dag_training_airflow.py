from airflow.decorators import dag, task, task_group 
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from datetime import timedelta, datetime
import os
from include.scripts.sql import sql
from include.scripts.python import worker


DEFAULT_ARGS = {
    'owner': 'Ned',
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5)
}

# table_name_db : value_in_data
CATEGORY_MAPPING = {'alcoholic':'Alcoholic beverages', 
                    'cereals_bakery':'Cereals and bakery products', 
                    'meats_poultry':'Meats and poultry'}

PG_HOOK = PostgresHook(postgres_conn_id='postgres')

TODAY_DATE = datetime.now().strftime("%Y%m%d")


@dag(default_args=DEFAULT_ARGS, start_date=days_ago(1), schedule_interval="50 11 * * *", default_view='graph', catchup=False)
def filtering_customer_consumption_backup():
    # is_file_available = FileSensor(
    #     task_id="is_file_available",
    #     fs_conn_id='file_path', #setup airflow conection with extra field is {"path":"./include/resources/data/raw/"}
    #     filepath=f'consumption_{datetime.now().strftime("%Y%m%d")}.csv',
    #     poke_interval=300,
    #     timeout=1500
    # )
    
    ### Check file is available IF yes continue flow, else wait for retry ###
    @task()
    def is_file_available(retries= 3, retry_delay=timedelta(minutes=5)):
        # path = f'./include/resources/data/raw/consumption_{TODAY_DATE}.csv'
        path = f'./include/resources/data/raw/consumption_yyyymmdd.csv'
        if os.path.exists(path):
            return path
        raise ValueError(f"File does not exist: {path}")
    
    ### Create DB for consumption tables ###
    @task()
    def create_db(table_name: str):
            PG_HOOK.run(sql.create_and_trunc_table(table_name))
    
    ### Read file and push to Xcom with json format (string) ###
    @task()
    def extract(file_patth):
        return worker.extract(file_patth)

    ### Read json format (string) to pd.Dataframe and spit to 3 csv files with each category. Finally store to TEMP folder  ###
    @task()
    def transform(data_json):
        file_paths = worker.transform(data_json)
        return file_paths
    
    ### Load data to datalake and delete temp files ###
    @task()
    def load(transformed_data_path: list):
        for path in transformed_data_path:
            worker.load(path, PG_HOOK)

    ### Check how many records are inserted? ###
    @task()
    def check_inserted_recs():
        query = (worker.check_records(CATEGORY_MAPPING.keys()))
        pg_conn = PG_HOOK.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(query)

        for content in cursor.fetchall():
            print(f'>>> IN table consumtion_{content[0]} is having {content[1]} records <<<')

    file_path = is_file_available()
    data_json = extract(file_path)
    transformed_data_path = transform(data_json)

    create_db = create_db.expand_kwargs([{'table_name': x} for x in CATEGORY_MAPPING.keys()])
    file_path >> [create_db, data_json] >> transformed_data_path >> load(transformed_data_path) >> check_inserted_recs()

training_dag = filtering_customer_consumption_backup()

    
    # @task_group
    # def transform_load(data_json):
    #     transformed_data_paths = transform(data_json)
    #     kwargs =  [{'transformed_data_path': path,'pg_hook': PG_HOOK} for path in transformed_data_paths]
    #     load_data = load.expand_kwargs(kwargs)
        
    #     transformed_data_paths >> load_data
    