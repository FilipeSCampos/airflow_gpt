from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
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

        # Salvando o DataFrame como um arquivo CSV (opcional)
        csv_filename = "countries_data.csv"
        df.to_csv(csv_filename, index=False)

        print(f"Dados transformados e salvos com sucesso em {csv_filename}")
        return df  # Retornar o DataFrame em vez do nome do arquivo
    else:
        print(f"Falha ao obter dados da API. Código de status: {response.status_code}")
        return None

def load_data_to_mongodb():
    df = extract_and_transform_data()
    if df is not None:
        mongo_hook = MongoHook(conn_id="mongo_default")  # Use a conexão definida no Airflow

        db = mongo_hook.get_conn().Teste_airflow  # Substitua pelo nome do seu banco de dados
        collection = db.airflow  # Substitua pelo nome da sua coleção

        records = df.to_dict(orient='records')
        try:
            collection.insert_many(records)
            print("Dados carregados com sucesso no MongoDB.")
        except Exception as e:
            print(f"Erro ao carregar dados no MongoDB: {str(e)}")

default_args = {
    'owner': 'filipe',
    'start_date': datetime(2023, 11, 8),
    'retries': 1,
}

dag = DAG(
    'extracao_transformacao_carregamento_mongo',
    default_args=default_args,
    schedule_interval=None,  # Defina a programação de acordo com sua necessidade
    catchup=False,
)

extract_and_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform_data,
    dag=dag,
)

load_to_mongodb_task = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_data_to_mongodb,
    dag=dag,
)

extract_and_transform_task >> load_to_mongodb_task
