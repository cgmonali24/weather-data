from airflow.hooks.postgres_hook import PostgresHook
import json

def insert_weather_data(weather_data,postgres_conn_id):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        for day in weather_data:
            if isinstance(day, str):
                day = eval(day)
            cursor.execute("""
                SELECT COUNT(*) FROM weather_data WHERE datetime = %s
            """, (day['datetime'],))
            count = cursor.fetchone()[0]
            
            if count > 0:
                cursor.execute("""
                    UPDATE weather_data
                    SET temperature = %s, humidity = %s
                    WHERE datetime = %s
                """, (day['temp'], day['humidity'], day['datetime']))
            else:
                cursor.execute("""
                    INSERT INTO weather_data (datetime, temperature, humidity)
                    VALUES (%s, %s, %s)
                """, (day['datetime'], day['temp'], day['humidity']))
        
        connection.commit()
        cursor.close()
        connection.close()

