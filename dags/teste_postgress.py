from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd

def extract_and_transform_data():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)

        print("Dados transformados com sucesso.")
        return df  # Agora retorna o DataFrame diretamente
    else:
        print(f"Falha ao obter dados da API. Código de status: {response.status_code}")
        return None

def load_data_to_postgres():
    df = extract_and_transform_data()
    if df is not None:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

        table_name = "airflow"  # Substitua pelo nome da sua tabela criada

        records = df.to_dict(orient='records')
        try:
            print("Inserindo dados no PostgreSQL...")
            postgres_hook.insert_rows(table=table_name, rows=records, replace=True)
            print("Dados carregados com sucesso no PostgreSQL.")
        except Exception as e:
            print(f"Erro ao carregar dados no PostgreSQL: {str(e)}")

default_args = {
    'owner': 'Filipe',
    'start_date': datetime(2023, 11, 8),
    'retries': 1,
}

dag = DAG(
    'extracao_transformacao_carregamento_postgres',
    default_args=default_args,
    schedule_interval=None,  # Define the schedule according to your needs
    catchup=False,
)

extract_and_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform_data,
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

extract_and_transform_task >> load_to_postgres_task
