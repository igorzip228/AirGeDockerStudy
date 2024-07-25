from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from great_expectations.data_context import get_context
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import Checkpoint
from datetime import datetime, timedelta
import logging

# Определение аргументов по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}


def log_table_names(**context):
    table_names = context['ti'].xcom_pull(task_ids='get_table_names')
    logging.info("Tables in the database: %s", table_names)


def run_great_expectations_check(**kwargs):
    # Путь к конфигурации Great Expectations
    ge_root_dir = "/opt/airflow/expectations"

    # Создаем контекст Great Expectations
    context = get_context(context_root_dir=ge_root_dir)

    # Получаем соединение с базой данных
    hook = PostgresHook(postgres_conn_id='airflow_db')
    conn_string = hook.get_uri()

    # Настраиваем источник данных, если он еще не существует
    if "my_postgres_datasource" not in context.list_datasources():
        datasource_config = {
            "name": "my_postgres_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": conn_string,
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }
        context.add_datasource(**datasource_config)

    # Создаем BatchRequest
    batch_request = BatchRequest(
        datasource_name="my_postgres_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_runtime_asset",
        runtime_parameters={
            "query": "SELECT * FROM information_schema.tables WHERE table_schema = 'public'"
        },
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    # Создаем или загружаем чекпоинт
    checkpoint_name = "my_checkpoint"
    if checkpoint_name not in context.list_checkpoints():
        checkpoint = Checkpoint(
            name=checkpoint_name,
            run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
            expectation_suite_name="my_suite",
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": "my_suite",
                }
            ],
        )
        context.add_checkpoint(checkpoint)
    else:
        checkpoint = context.get_checkpoint(checkpoint_name)

    # Выполняем проверку
    results = checkpoint.run()

    # Логируем результат
    logging.info(f"Validation results: {results.success}")

    # Если проверка не прошла, вызываем исключение
    if not results['success']:
        raise ValueError("Great Expectations validation failed!")


# Определение DAG
with DAG(
        'list_db_tables_with_ge',
        default_args=default_args,
        description='A DAG to list DB tables and run Great Expectations checks',
        schedule_interval=None,
        catchup=False,
) as dag:
    # Задача для выполнения SQL-запроса на получение списка таблиц
    get_table_names = PostgresOperator(
        task_id='get_table_names',
        postgres_conn_id='airflow_db',
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

    # Задача для выполнения проверки Great Expectations
    ge_check = PythonOperator(
        task_id='run_great_expectations_check',
        python_callable=run_great_expectations_check,
        provide_context=True,
    )

    # Определение последовательности выполнения задач
    get_table_names >> log_tables >> ge_check