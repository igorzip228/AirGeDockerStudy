from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Определение аргументов по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}


# Функция для вывода результата SQL-запроса
def log_table_names(**context):
    table_names = context['ti'].xcom_pull(task_ids='get_table_names')
    logging.info("Tables in the database: %s", table_names)


# Создание DAG
with DAG(
        'list_db_tables',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    # Задача для выполнения SQL-запроса на получение списка таблиц
    get_table_names = PostgresOperator(
        task_id='get_table_names',
        postgres_conn_id='airflow_db',  # Убедитесь, что этот ID соединения настроен в Airflow
        sql="""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
        """,
    )

    # Задача для вывода имен таблиц в лог
    log_tables = PythonOperator(
        task_id='log_table_names',
        python_callable=log_table_names,
        provide_context=True,
    )

    # Определение последовательности выполнения задач
    get_table_names >> log_tables
