import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def fetch_weather_data_from_db(sql_query, export_path, postgres_conn_id='my_sql_connection'):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()

    df = pd.read_sql_query(sql_query, conn)
    df.to_csv(export_path, index=False)
    print(f"Data exported to {export_path}")
