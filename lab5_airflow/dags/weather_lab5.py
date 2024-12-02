from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import json

postgres_conn_id = 'postgres'
city_name = 'Portland'
api_key = 'f47b7320ff3f8199243a2954fadeccd4'

@dag(
    dag_id='lab5',
    schedule="@daily",
    start_date=datetime(2024, 11, 28),
    catchup=False,
    default_args={"owner": "Amoako", "retries": 2},
    tags=["Lab5"],
)
def weather_data():
    # Task 1: Check if API is active
    is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_posts',
        endpoint=f'2.5/weather?q={city_name}&appid={api_key}',
        poke_interval=5,
        timeout=20,
    )

    # Task 2: Get weather data
    get_weather_data = SimpleHttpOperator(
        task_id='get_weather_data',
        http_conn_id='api_posts',
        endpoint=f'2.5/weather?q={city_name}&appid={api_key}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    # Task 3: Transform data 
    @task()
    def transform_data(current_weather):
        ###Transform the weather data.
        kevin = current_weather['main']['temp']
        fahrenheit = ((kevin - 273.15) * 1.8) + 32

        transformed_data = {
            'city': current_weather['name'],
            'temperature': fahrenheit,
            'pressure': current_weather['main']['pressure'],
            'humidity': current_weather['main']['humidity'],
        }
        return transformed_data  



    # Task 4: Load weather data into Postgres
    @task()
    def load_weather_data(transformed_data):
        # Load the transformed data into the database.
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            city VARCHAR(255),
            temperature FLOAT,
            pressure INT,
            humidity INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO daily_weather (city, temperature, pressure, humidity)
        VALUES (%s, %s, %s, %s);
        """, (
            transformed_data['city'],
            transformed_data['temperature'],
            transformed_data['pressure'],
            transformed_data['humidity'],
        ))

        conn.commit()
        cursor.close()

    # Task Dependencies
    is_api_active >> get_weather_data

    transformed = transform_data(get_weather_data.output)  
    transformed >> load_weather_data(transformed)

weather_data()
