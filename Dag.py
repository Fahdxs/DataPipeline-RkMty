import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup 
from tasks.check_data import check_data
from tasks.converting_to_csv import character_json_parse,episode_json_parse,location_json_parse
from tasks.loading_tasks import load_to_pg
from tasks.bronze_qc import b_quality_check
from tasks.pulling_data import fetch_data
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.utils.trigger_rule import TriggerRule




with DAG(
    'dp_ricknmorty',
    default_args= {
        'owner' : 'airflow',
        'start_date' : datetime(2025, 4, 12),
        'retries' : 0
    },
    schedule_interval= '@daily',
    catchup= False
) as dag:
    
    with TaskGroup(group_id= 'character_tasks') as character_tasks:

        pre_ingestion = BranchPythonOperator(
            task_id= 'character_pre_ingestion_task',
            python_callable= check_data,
            op_kwargs= {
                'path' : '/opt/airflow/data/',
                'endpoint' : 'character'
            }
        )

        character_api = PythonOperator(
            task_id= 'fetch_character_data',
            python_callable= fetch_data,
            op_kwargs= {'url' : 'https://rickandmortyapi.com/api/' , 'endpoint' : 'character'},
            execution_timeout=timedelta(minutes=2)
        )

        character_csv_generator = PythonOperator(
            task_id= 'character_csv_generator',
            python_callable= character_json_parse,
            op_kwargs= {
                'pwd' : '/opt/airflow/data/',
                'endpoint' : 'character'
            }
        )

        pre_ingestion >> [character_api, character_csv_generator]



    with TaskGroup(group_id= 'location_tasks') as location_tasks:

        pre_ingestion = BranchPythonOperator(
            task_id= 'location_pre_ingestion_task',
            python_callable= check_data,
            op_kwargs= {
                'path' : '/opt/airflow/data/',
                'endpoint' : 'location'
            }
        )

        location_api = PythonOperator(
            task_id= 'fetch_location_data',
            python_callable= fetch_data,
            op_kwargs= {'url' : 'https://rickandmortyapi.com/api/' , 'endpoint' : 'location'},
            execution_timeout=timedelta(minutes=2)
        )

        location_csv_generator = PythonOperator(
            task_id= 'location_csv_generator',
            python_callable= location_json_parse,
            op_kwargs= {
                'pwd' : '/opt/airflow/data/',
                'endpoint' : 'location'
            }
        )

        pre_ingestion >> [location_api, location_csv_generator] 
    
    with TaskGroup(group_id= 'episode_tasks') as episode_tasks:

        pre_ingestion = BranchPythonOperator(
            task_id= 'episode_pre_ingestion_task',
            python_callable= check_data,
            op_kwargs= {
                'path' : '/opt/airflow/data/',
                'endpoint' : 'episode'
            }
        )

        episode_api = PythonOperator(
            task_id= 'fetch_episode_data',
            python_callable= fetch_data,
            op_kwargs= {'url' : 'https://rickandmortyapi.com/api/' , 'endpoint' : 'episode'},
            execution_timeout=timedelta(minutes=2)
        )

        episode_csv_generator = PythonOperator(
            task_id= 'episode_csv_generator',
            python_callable= episode_json_parse,
            op_kwargs= {
                'pwd' : '/opt/airflow/data/',
                'endpoint' : 'episode'
            }
        )

        pre_ingestion >> [episode_api, episode_csv_generator]

    with TaskGroup(group_id= 'bronze_task') as bronze_task:

        ddl_bronze_character = SQLExecuteQueryOperator(

            task_id= 'ddl_bronze_character',
            conn_id= 'postgre_db',
            trigger_rule= TriggerRule.ONE_SUCCESS,
            sql= '''
                DROP TABLE IF EXISTS bronze.ricknmorty_character;
                CREATE TABLE IF NOT EXISTS bronze.ricknmorty_character (
                    id VARCHAR(20),
                    name VARCHAR(40),
                    status VARCHAR,
                    type VARCHAR,
                    gender VARCHAR(10),
                    image VARCHAR(100)
                    );
                '''
        )


        ddl_bronze_episode = SQLExecuteQueryOperator(

            task_id= 'ddl_bronze_episode',
            conn_id= 'postgre_db',
            trigger_rule= TriggerRule.ONE_SUCCESS,
            sql= '''
                DROP TABLE IF EXISTS bronze.ricknmorty_episode;
                CREATE TABLE IF NOT EXISTS bronze.ricknmorty_episode(
                    id INTEGER,
                    name VARCHAR(50),
                    air_date VARCHAR(20),
                    episode VARCHAR(20),
                    char_ids VARCHAR
                );
                '''
        )

        ddl_bronze_location = SQLExecuteQueryOperator(

            task_id= 'ddl_bronze_location',
            conn_id= 'postgre_db',
            trigger_rule= TriggerRule.ONE_SUCCESS,
            sql= '''
                DROP TABLE IF EXISTS bronze.ricknmorty_location;
                CREATE TABLE IF NOT EXISTS bronze.ricknmorty_location(
                    id INTEGER,
                    name VARCHAR,
                    type VARCHAR,
                    dimension VARCHAR,
                    char_ids VARCHAR
                );
                '''
        )

        insert_bronze_character = PythonOperator(

            task_id= 'insert_bronze_character',
            python_callable= load_to_pg, 
            op_kwargs= {
                'file' : '/opt/airflow/data/',
                'endpoint' : 'character',
                'tablename' : 'bronze.ricknmorty_character'
            }
        )

        insert_bronze_episode = PythonOperator(

            task_id= 'insert_bronze_episode',
            python_callable= load_to_pg,
            op_kwargs= {
                'file' : '/opt/airflow/data/',
                'endpoint' : 'episode',
                'tablename' : 'bronze.ricknmorty_episode'
            }
        )

        insert_bronze_location = PythonOperator(

            task_id= 'insert_bronze_location',
            python_callable= load_to_pg,
            op_kwargs= {
                'file' : '/opt/airflow/data/',
                'endpoint' : 'location',
                'tablename' : 'bronze.ricknmorty_location'
            }
        )

        quality_check_bcharacter = PythonOperator(
            task_id= 'quality_check_bcharacter',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'ricknmorty_character',
                'schema' : 'bronze'
            }
        )

        quality_check_blocation = PythonOperator(
            task_id= 'quality_check_blocation',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'ricknmorty_location',
                'schema' : 'bronze'
            }
        )

        quality_check_bepisode = PythonOperator(
            task_id= 'quality_check_bepisode',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'ricknmorty_episode',
                'schema' : 'bronze'
            }
        )
    
    with TaskGroup(group_id= 'silver_task') as silver_task:

        ddl_silver_location = SQLExecuteQueryOperator(

            task_id= 'ddl_silver_location',
            conn_id= 'postgre_db',
            sql= '''
               DROP TABLE IF EXISTS silver.dim_location;
                CREATE TABLE IF NOT EXISTS silver.dim_location(
                    id INTEGER,
                    location_name VARCHAR(300),
                    type VARCHAR(300),
                    dimension VARCHAR(300),
                    PRIMARY KEY(id)
                );
                '''
        )

        ddl_silver_character = SQLExecuteQueryOperator(

            task_id= 'ddl_silver_character',
            conn_id= 'postgre_db',
            sql= '''
                DROP TABLE IF EXISTS silver.dim_character;
                CREATE TABLE IF NOT EXISTS silver.dim_character(
                    id INTEGER,
                    character_name VARCHAR(100),
                    status VARCHAR(100),
                    type VARCHAR(250),
                    gender VARCHAR(10),
                    PRIMARY KEY(id)
                );
                '''
        )

        ddl_silver_episode = SQLExecuteQueryOperator(

            task_id= 'ddl_silver_episode',
            conn_id= 'postgre_db',
            sql= '''
                DROP TABLE IF EXISTS silver.dim_episode;
                CREATE TABLE IF NOT EXISTS silver.dim_episode(
                    id INTEGER,
                    episode_name VARCHAR(100),
                    air_date DATE,
                    episode VARCHAR(100),
                    PRIMARY KEY(id)
                );
                '''
        )

        ddl_silver_episodebridge = SQLExecuteQueryOperator(

            task_id= 'ddl_silver_episodebrige',
            conn_id= 'postgre_db',
            sql= '''
                DROP TABLE IF EXISTS silver.bridge_episode_character;
                CREATE TABLE IF NOT EXISTS silver.bridge_episode_character(
                episode_id INTEGER,
                character_id INTEGER,
                PRIMARY KEY(episode_id, character_id)
                );
                '''
        )

        ddl_silver_locationbridge = SQLExecuteQueryOperator(

            task_id= 'ddl_silver_locationbrige',
            conn_id= 'postgre_db',
            sql= '''
                DROP TABLE IF EXISTS silver.bridge_location_character;
                CREATE TABLE IF NOT EXISTS silver.bridge_location_character(
                location_id INTEGER,
                character_id INTEGER,
                PRIMARY KEY(location_id, character_id)
                    );
                '''
        )


        transform_and_insert_sc = SQLExecuteQueryOperator(

            task_id= 'transform_and_insert_sc',
            conn_id= 'postgre_db',
            sql= '''
                INSERT INTO silver.dim_character
                SELECT 
                    id :: INTEGER,
                    TRIM(COALESCE(name,'unknown')) as customer_name,
                    TRIM(COALESCE(status,'unknown')) as status,
                    TRIM(COALESCE(type,'unkown')) as type,
                    CASE 
                        WHEN UPPER(gender) IN ('MALE','M')
                        THEN 'male'
                        WHEN UPPER(gender) IN ('FEMALE','F')
                        THEN 'female'
                    ELSE 'unkown' END as gender
                FROM bronze.ricknmorty_character;
                '''
        )

        transform_and_insert_se = SQLExecuteQueryOperator(

            task_id= 'transform_and_insert_se',
            conn_id= 'postgre_db',
            sql= """
                INSERT INTO silver.dim_episode
                SELECT 
                    id :: INTEGER,
                    TRIM(COALESCE(name,'unkown')) as episode_name,
                    TO_DATE(air_date, 'FMMonth DD, YYYY') AS air_date,
                    TRIM(COALESCE(episode,'unknown')) as episode
                FROM bronze.ricknmorty_episode;
                """
            )
            
        transform_and_insert_sl = SQLExecuteQueryOperator(

            task_id= 'transform_and_insert_sl',
            conn_id= 'postgre_db',
            sql= """
                INSERT INTO silver.dim_location
                SELECT 
                    id :: INTEGER,
                    TRIM(COALESCE(name)) as location_name,
                    TRIM(COALESCE(type,'unknown')) AS type,
                    TRIM(COALESCE(dimension,'unkown')) as dimension
                FROM bronze.ricknmorty_location;
                """
            )
        
        transform_and_insert_slb = SQLExecuteQueryOperator(

            task_id= 'transform_and_insert_slb',
            conn_id= 'postgre_db',
            sql= """
               INSERT INTO silver.bridge_location_character
                SELECT
                    id as location_id,
                    UNNEST(STRING_TO_ARRAY(REPLACE(REPLACE(REPLACE(char_ids,'[',''),']',''),'''', ''),',')) :: INTEGER AS character_id
                FROM bronze.ricknmorty_location;
                """
            )

        transform_and_insert_seb = SQLExecuteQueryOperator(

            task_id= 'transform_and_insert_seb',
            conn_id= 'postgre_db',
            sql= """
                INSERT INTO silver.bridge_episode_character
                SELECT
                    id AS episode,
                    UNNEST(STRING_TO_ARRAY(REPLACE(REPLACE(REPLACE(char_ids,'[',''),']',''),'''', ''),',')) :: INTEGER AS character_id
                FROM bronze.ricknmorty_episode;
                """
            )

        quality_check_sepisode = PythonOperator(
            task_id= 'quality_check_sepisode',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'dim_episode',
                'schema' : 'silver'
            }
        )

        quality_check_slocation = PythonOperator(
            task_id= 'quality_check_slocation',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'dim_location',
                'schema' : 'silver'
            }
        )

        quality_check_scharacter = PythonOperator(
            task_id= 'quality_check_scharacter',
            python_callable= b_quality_check,
            op_kwargs= {
                'name' : 'dim_character',
                'schema' : 'silver'
            }
        )

    with TaskGroup(group_id= 'gold_task') as gold_task:

        create_view_cl = SQLExecuteQueryOperator(

            task_id= 'create_view_cl',
            conn_id= 'postgre_db',
            sql= """
                DROP VIEW IF EXISTS gold.character_location_view;
                CREATE VIEW gold.character_location_view AS 
                SELECT
                    sl.id as location_id,
                    sc.character_name,
                    sl.location_name,
                    sc.gender,
                    sl.type as location_type,
                    sl.dimension as location_dimension,
                    sc.status,
                    sc.type as character_type
                FROM silver.dim_character sc
                JOIN silver.bridge_location_character bl
                ON sc.id = bl.location_id 
                JOIN silver.dim_location sl
                ON bl.location_id = sl.id;
                """
            )
        
        create_view_ce = SQLExecuteQueryOperator(

            task_id= 'create_view_ce',
            conn_id= 'postgre_db',
            sql= """
                DROP VIEW IF EXISTS gold.character_episode_view;
                CREATE VIEW gold.character_episode_view AS
                SELECT
                    sc.id as character_id,
                    se.id as episode_id,
                    sc.character_name,
                    se.episode_name,
                    sc.status,
                    sc.type as character_type,
                    sc.gender,
                    se.air_date,
                    se.episode
                FROM silver.dim_character sc
                JOIN silver.bridge_episode_character bc
                ON sc.id = bc.character_id 
                JOIN silver.dim_episode se
                ON bc.episode_id = se.id;
                """
            )
        
        create_view_cel = SQLExecuteQueryOperator(

            task_id= 'create_view_cel',
            conn_id= 'postgre_db',
            sql= """
                DROP VIEW IF EXISTS gold.character_episode_location_view;
                CREATE VIEW gold.character_episode_location_view AS
                SELECT
                    sc.id as character_id,
                    se.id as episode_id,
                    sl.id as location_id,
                    sc.character_name,
                    se.episode_name,
                    sl.location_name,
                    sc.status as character_status,
                    sc.type as character_type,
                    sc.gender,
                    se.air_date,
                    se.episode,
                    sl.type as location_type,
                    sl.dimension as location_dimension
                FROM silver.dim_character sc
                JOIN silver.bridge_episode_character bc
                ON sc.id = bc.character_id 
                JOIN silver.dim_episode se
                ON bc.episode_id = se.id
                JOIN silver.bridge_location_character bl
                ON bl.character_id = sc.id
                JOIN silver.dim_location sl
                ON bl.location_id = sl.id;
                """
            )

    
    character_tasks >> ddl_bronze_character >> insert_bronze_character >> quality_check_bcharacter >> ddl_silver_character >> transform_and_insert_sc >> quality_check_scharacter
    episode_tasks >> ddl_bronze_episode >> insert_bronze_episode >> quality_check_bepisode >> ddl_silver_episode >> transform_and_insert_se >> quality_check_sepisode
    location_tasks >> ddl_bronze_location >> insert_bronze_location >> quality_check_blocation >> ddl_silver_location >> transform_and_insert_sl >> quality_check_slocation

    quality_check_bepisode >> ddl_silver_episodebridge >> transform_and_insert_seb
    quality_check_blocation >> ddl_silver_locationbridge >> transform_and_insert_slb

    quality_check_scharacter >> create_view_ce
    quality_check_sepisode >> create_view_ce
    transform_and_insert_seb >> create_view_ce

    quality_check_scharacter >> create_view_cl
    quality_check_slocation >> create_view_cl
    transform_and_insert_slb >> create_view_cl

    quality_check_scharacter >> create_view_cel
    quality_check_sepisode >> create_view_cel
    transform_and_insert_seb >> create_view_cel
    quality_check_slocation >> create_view_cel
    transform_and_insert_slb >> create_view_cel



