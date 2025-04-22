import os 
from airflow.hooks.postgres_hook import PostgresHook
import logging


def b_quality_check(name, schema):


    tablename = f'{schema}.{name}'
    hook = PostgresHook(postgres_conn_id= 'postgre_db')


    quality_check1 = f'''SELECT COUNT(*) AS total_rows
                        FROM {tablename};'''  

    result = hook.get_records(quality_check1)

    if result[0][0] > 0:
        
        quality_check2 = f'''
                            SELECT 
                                SUM(indicator) 
                            FROM (
                            SELECT
                                id,
                                CASE WHEN COUNT(*) = 1 THEN 0 ELSE 1 END AS indicator
                            FROM {tablename}
                            GROUP BY id) as dc; '''
        
        result = hook.get_records(quality_check2)

        if result[0][0] == 0:

            quality_check3 = f'''SELECT 
                                    COUNT(*) AS total_null_id
                                FROM {tablename}
                                WHERE id IS NULL;'''

            result =  hook.get_records(quality_check3)

            if result[0][0] == 0:

                logging.info(f'Successful quality check for table {tablename}')
                return "Quality check passed"

        
            else:

                logging.error(f"Table {tablename} have null values")
                raise ValueError(f"Quality check failed: {tablename} have null values")
        
        else:

            logging.error(f"Table {tablename} have duplicate ids")
            raise ValueError(f"Quality check failed: {tablename} have duplicate ids")
    
    else:

        logging.error(f"Table {tablename} is empty")
        raise ValueError(f"Quality check failed: {tablename} has no rows")









