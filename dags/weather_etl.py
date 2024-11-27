from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json



POSTGRES_CONN_ID='lab_5_connections'
city_name = 'munich'
API_CONN_ID='openweather_api'
API_key = 'f47b7320ff3f8199243a2954fadeccd4'

default_args={
    'owner':'amoako',
    'start_date':days_ago(1)
}

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        ## https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}
        endpoint=f'/data/2.5/weather?q={city_name}&appid={API_key}'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")


    @task()
    def transform_weather_data(weather_data):
        #Transforming the data
        current_weather = weather_data['current_weather']

        ##Kevin to Fahrenheit
        kevin = current_weather['main']['temp']
        fahrenheit = ((kevin - 273.15)*1.8) + 32
        


        transformed_data = {
            'city': current_weather['name'],
            'temperature': fahrenheit,
            'pressure': current_weather['main']['pressure'],
            'humidity': current_weather['main']['humidity'],
            'timestamps': current_weather['timestamps']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            city VARCHAR(255),
            temperature FLOAT,
            humidity INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (city, temperature, humidity, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['city'],
            transformed_data['temperature'],
            transformed_data['humidity'],
            transformed_data['timestamp']
        ))

        conn.commit()
        cursor.close()

    ## DAG Worflow- ETL Pipeline
    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)