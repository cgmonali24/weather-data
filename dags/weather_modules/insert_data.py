from airflow.hooks.postgres_hook import PostgresHook
import json

def insert_weather_data(weather_data,postgres_conn_id, **kwargs):
    """
            Inserts weather data into a PostgreSQL database.
            This function takes weather data and inserts it into a PostgreSQL database table 
            If a record with the same datetime already exists, it updates the temperature and humidity values.
            Args:
                weather_data (list of dict): A list of dictionaries containing weather data. Each dictionary have
                                            the keys 'datetime', 'temp', and 'humidity'.
                postgres_conn_id (str): The connection ID for the PostgreSQL database.
            Raises:
                TypeError: If weather_data is None.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    try:
        if weather_data is None:
            raise TypeError("Weather data is None, cannot process it.")

        data_to_insert = [(day["datetime"], day["temp"], day["humidity"]) for day in weather_data]
        cursor.executemany("""
            INSERT INTO weather_data (datetime, temperature, humidity)
            VALUES (%s, %s, %s)
            ON CONFLICT (datetime) DO UPDATE
            SET temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity
            """, data_to_insert)
    except TypeError as e:
        print(f"Error inserting data: {e}") 
        pass
    
    connection.commit()
    cursor.close()
    connection.close()

