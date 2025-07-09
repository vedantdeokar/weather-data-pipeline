from airflow import DAG
from datetime import timedelta, datetime, timezone
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup 
from airflow.operators.python_operator import PythonOperator 
import pandas as pd 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from airflow.decorators import task
import requests



CITIES= ["mumbai","chennai","delhi","kolkata"]


@task
def extract_data(city):
    url= f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid=78babdb8f12eb96f3d8fe1f02f3e6f40"
    response = requests.get(url)
    return response.text

def convert_to_celsius(kelvin):
    return kelvin - 273.15

@task
def transform_data(response,city):
    data = json.loads(response)
    content = {
        'longitude': data['coord']['lon'],
        'latitude': data['coord']['lat'],
        'weather': data['weather'][0]['main'],
        'weather_description': data['weather'][0]['description'],
        'temperature': convert_to_celsius(data['main']['temp']),
        'feels_like': convert_to_celsius(data['main']['feels_like']),
        'temp_min': convert_to_celsius(data['main']['temp_min']),
        'temp_max': convert_to_celsius(data['main']['temp_max']),
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'wind_speed': data['wind']['speed'],
        'record_time': str(datetime.utcfromtimestamp(data['dt']).strftime("%Y-%m-%d %H:%M:%S")),
        'sunrise': str(datetime.utcfromtimestamp(data['sys']['sunrise']).strftime("%Y-%m-%d %H:%M:%S")),
        'sunset': str(datetime.utcfromtimestamp(data['sys']['sunset']).strftime("%Y-%m-%d %H:%M:%S")),
        'country': data['sys']['country'],
        'city': data['name'],
        'record_date': str(datetime.now().strftime("%Y-%m-%d")) 
    }

    
    df= pd.DataFrame([content])
    df.to_csv(f"weather_data_{city}.csv", index=False, header=False)
    return f"weather_data_{city}.csv"


@task
def load_data(path):
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    hook.copy_expert(sql= "COPY weather_data FROM stdin WITH DELIMITER as ',' " ,
            filename= "/home/ubuntu/"+path )



def upload_to_s3(task_instance):
    data= task_instance.xcom_pull(task_ids='join_data')
    df = pd.DataFrame(data, columns=["city", "state", "population", "area_sq_km", "koppen_climate_classification","elevation_m",
                                    "green_cover_percentage", "population_density",
                                    "longitude", "latitude", "weather", "weather_description",
                                    "temperature", "feels_like", "temp_min", "temp_max",
                                    "humidity","pressure", "wind_speed","record_time","sunrise","sunset", "country","record_date"])

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    name = 'hour_' + datetime.now().strftime("%H")
    folder = 'weather_data_' + datetime.now().strftime("%Y-%m-%d")
    key = f"{folder}/{name}.csv" 
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name='open-weather-data-vedant',
        replace=True
    )







default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2025, 1, 1, 0, 0, 0),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=2)
}


with DAG('weather_dag',
    default_args= default_args,
    schedule_interval= "0 * * * *",
    catchup= False,
    ) as dag:

    start_pipeline= EmptyOperator(
        task_id ="start_pipeline"
    )


    with TaskGroup(group_id="setup_tables") as setup_tables:

        create_table_1= PostgresOperator(
            task_id="create_table_1",
            postgres_conn_id= "postgres_conn",
            sql= """ CREATE TABLE IF NOT EXISTS cities(
            city VARCHAR(50),
            state VARCHAR(50),
            population INT,
            area_sq_km NUMERIC,
            koppen_climate_classification TEXT,
            elevation_m INT,
            green_cover_percentage INT,
            population_density INT
            );
            """
        )

        truncate_table_1 = PostgresOperator(
            task_id="truncate_table_1",
            postgres_conn_id="postgres_conn",
            sql="""TRUNCATE TABLE cities;"""
        )

        table1_s3_to_postgres= PostgresOperator(
            task_id="table1_s3_to_postgres",
            postgres_conn_id= 'postgres_conn',
            sql= """ SELECT aws_s3.table_import_from_s3('cities' , '' , '(format csv, DELIMITER '','', HEADER true)' ,
                'open-weather-data-vedant' , 'city_data.csv' , 'us-east-1');
            """
        )

        create_table_2 = PostgresOperator(
            task_id ="create_table_2",
            postgres_conn_id= "postgres_conn",
            sql= """  
            CREATE TABLE IF NOT EXISTS weather_data (
            longitude         REAL,
            latitude          REAL,
            weather           TEXT,
            weather_description TEXT,
            temperature       REAL,
            feels_like        REAL,
            temp_min          REAL,
            temp_max          REAL,
            humidity          REAL,
            pressure          REAL,
            wind_speed        REAL,
            record_time              TEXT,
            sunrise           TEXT,
            sunset            TEXT,
            country           TEXT,
            city              TEXT,
            record_date              TEXT
            );

            """
        )

        truncate_table_2 = PostgresOperator(
            task_id="truncate_table_2",
            postgres_conn_id="postgres_conn",
            sql="""TRUNCATE TABLE weather_data;"""
        )

        create_table_1 >> truncate_table_1 >> table1_s3_to_postgres
        create_table_2 >> truncate_table_2 

    is_weather_api_ready = HttpSensor(
            task_id='is_weather_api_ready',
            http_conn_id='weather_api',
            endpoint="/data/2.5/weather?q=houston&appid=78babdb8f12eb96f3d8fe1f02f3e6f40"
        )

    with TaskGroup(group_id="etl",tooltip="parallel tasks of etl and s3_to_postgres") as etl:
        for city in CITIES:
            extract = extract_data.override(task_id=f"extract_data_{city}")(city)
            transform = transform_data.override(task_id=f"transform_data_{city}")(extract,city)
            load = load_data.override(task_id=f"load_data_{city}")(transform)


    # join data
    join_data = PostgresOperator(
        task_id = "join_data",
        postgres_conn_id = "postgres_conn",
        sql = """
        SELECT 
        c.city , c.state , c.population , c.area_sq_km, c.koppen_climate_classification,
        c.elevation_m, c.green_cover_percentage, c.population_density,
        wd.longitude, wd.latitude, wd.weather, wd.weather_description , wd.temperature, 
        wd.feels_like, wd.temp_min, wd.temp_max, wd.humidity,wd.pressure, wd.wind_speed,
        wd.record_time, wd.sunrise, wd.sunset, wd.country, wd.record_date
        FROM weather_data wd
        INNER JOIN cities c ON wd.city = c.city;
        """,
        do_xcom_push= True
    )

    upload_joined_data = PythonOperator(
        task_id= 'upload_joined_data',
        python_callable = upload_to_s3
    )

    end_pipeline = EmptyOperator(
        task_id = 'end_pipeline'
    )

    start_pipeline >> setup_tables >> is_weather_api_ready >> etl >> join_data >> upload_joined_data >> end_pipeline