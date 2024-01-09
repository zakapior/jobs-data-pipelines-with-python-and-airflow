import datetime as dt
import logging
import os

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from custom.operator_wwr import WeWorkRemotelyGetJobsOperator
from custom.operator_json2sql import Json2SqlOperator


with DAG(
    dag_id="weworkremotely_get_jobs",
    start_date=dt.datetime(2023, 10, 13),
    schedule_interval="@daily",
    tags=['jakluz-de2.3'],
    catchup=False
) as dag:
    WeWorkRemotelyGetJobsOperator(
        task_id="get_jobs",
        conn_id="weworkremotely",
        start_date="{{ ds }}",
        output_path="/tmp/job_offers/data/weworkremotely.json",
    )

with DAG(
    dag_id="wwr_parse_and_upload",
    start_date=dt.datetime(2023, 1, 1),
    end_date=dt.datetime(2024, 1, 1),
    schedule_interval="@daily",
    template_searchpath='/tmp/job_offers/data/',
    tags=['jakluz-de2.3'],
    catchup=True
) as dag:

    json2sql = Json2SqlOperator(
        task_id="json2sql",
        start_date="{{ data_interval_start }}",
        end_date="{{ data_interval_end }}",
        service_name='weworkremotely',

    )


    def _upload_to_db(sql_filename, conn_id):
        logger = logging.getLogger(__name__)
        hook = PostgresHook(conn_id)
        logger.info(hook.get_uri())

        if os.path.isfile(sql_filename):
            logger.info("File found: %s", sql_filename)

            with open(sql_filename) as file:
                hook.run(file)
        else:
            logger.info("File not found: %s", sql_filename)


    upload_to_db = PythonOperator(
        task_id="upload_to_db",
        python_callable=_upload_to_db,
        op_kwargs = {
            'sql_filename': '/tmp/job_offers/data/weworkremotely-{{ data_interval_start }}.sql',
            'conn_id': 'pg-job_applications'
        }
    )

    json2sql >> upload_to_db
