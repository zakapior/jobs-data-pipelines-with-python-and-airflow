import datetime as dt
import logging
import os

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from custom.operator_remotive import RemotiveGetJobsOperator
from custom.sensor_remotive import RemotiveNewJobsSensor
from custom.operator_json2sql import Json2SqlOperator

with DAG(
    dag_id="remotive_get_jobs",
    start_date=dt.datetime(2023, 10, 13),
    schedule_interval="@daily",
    tags=['jakluz-de2.3'],
    catchup=False
) as dag:
    RemotiveGetJobsOperator(
        task_id="get_jobs",
        conn_id="remotive",
        start_date="{{ ds }}",
        output_path="/tmp/job_offers/data/remotive.json",
    )

with DAG(
    dag_id="remotive_parse_and_upload",
    start_date=dt.datetime(2023, 1, 1),
    end_date=dt.datetime(2024, 1, 1),
    schedule_interval="@daily",
    template_searchpath='/tmp/job_offers/data/',
    tags=['jakluz-de2.3'],
    catchup=True
) as dag:
    look_for_new_jobs = RemotiveNewJobsSensor(
        task_id="look_for_new_jobs",
        start_date="{{ data_interval_start }}",
        end_date="{{ data_interval_end }}",
        poke_interval=3600,
        timeout=60*60*24*2
    )

    json2sql = Json2SqlOperator(
        task_id="json2sql",
        start_date="{{ data_interval_start }}",
        end_date="{{ data_interval_end }}",
        service_name='remotive',

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
            'sql_filename': '/tmp/job_offers/data/remotive-{{ data_interval_start }}.sql',
            'conn_id': 'pg-job_applications'
        }
    )

    look_for_new_jobs >> json2sql >> upload_to_db
