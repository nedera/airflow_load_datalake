
def create_and_trunc_table(table_name):
    # category, sub_category, aggregation_date, millions_of_dollar, pipeline_exc_datetime
    table_name = f'consumption_{table_name}_yyyymmdd'
    return f'''
    DROP TABLE IF EXISTS {table_name} ;
    CREATE TABLE IF NOT EXISTS {table_name}(
        category VARCHAR(50),
        sub_category VARCHAR(20),
        aggregation_date DATE,  -- Using DATE data type for yyyy/mm/dd format
        millions_of_dollar INT,  -- Using NUMERIC data type for monetary values
        pipeline_exc_datetime TIMESTAMP  -- Using TIMESTAMP data type for yyyy/mm/dd HH:mm:ss format
    );
    
    TRUNCATE TABLE {table_name};
    '''