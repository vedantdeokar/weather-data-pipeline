from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook

def validate_load(**context):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    count = hook.get_first("SELECT COUNT(*) FROM temp_weather_data")[0]
    if count == 0:
        raise ValueError("No data loaded into temp table!")
    context['ti'].xcom_push(key='row_count', value=count)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True
}

with DAG(
    'daily_redshift_load',
    default_args=default_args,
    schedule_interval='30 23 * * *',  
    catchup=False,
    max_active_runs=1
) as dag:

    create_temp_table = PostgresOperator(
        task_id='create_temp_table',
        postgres_conn_id='redshift_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS temp_weather_data (
            city VARCHAR(50),
            state VARCHAR(50),
            population INT,
            area_sq_km INT,
            koppen_climate_classification TEXT,
            elevation_m INT,
            green_cover_percentage INT,
            population_density INT,
            longitude REAL,
            latitude REAL,
            weather VARCHAR(50),
            weather_description VARCHAR(100),
            temperature REAL,
            feels_like REAL,
            temp_min REAL,
            temp_max REAL,
            humidity REAL,
            pressure REAL,
            wind_speed REAL,
            record_time TIMESTAMP,
            sunrise TIMESTAMP,
            sunset TIMESTAMP,
            country VARCHAR(10),
            record_date DATE  
        );
        """
    )

    load_s3_to_redshift = PostgresOperator(
        task_id='load_s3_to_redshift',
        postgres_conn_id='redshift_conn',  
        sql="""
        COPY temp_weather_data
        FROM 's3://open-weather-data-vedant/weather_data_2025-07-03/'
        IAM_ROLE 'arn:aws:iam::794038256477:role/RedshiftS3Access'
        FORMAT CSV
        DELIMITER ','
        IGNOREHEADER 1
        REGION 'us-east-1'
        TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
        DATEFORMAT 'auto'
        NULL AS ''
        MAXERROR 5;
        """,
        autocommit=True
    )


    merge_data = PostgresOperator(
        task_id='merge_data',
        postgres_conn_id='redshift_conn',
        sql="""
        INSERT INTO weather_data
        SELECT * FROM temp_weather_data;

        """
    )

    drop_temp_table = PostgresOperator(
        task_id='drop_temp_table',
        postgres_conn_id='redshift_conn',
        sql="""
        DROP TABLE IF EXISTS temp_weather_data;
        """
    )
    

    optimize_table = PostgresOperator(
        task_id='optimize_table',
        postgres_conn_id='redshift_conn',
        sql="""
        ANALYZE weather_data;
        """
    )

    create_temp_table >> load_s3_to_redshift >> merge_data >> drop_temp_table >> optimize_table

