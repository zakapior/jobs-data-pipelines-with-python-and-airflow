"""
This is a module with the Jobicy operator implementation for Apache Airflow.
"""
import json
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from custom.hook_jobicy import JobicyHook


class JobicyGetJobsOperator(BaseOperator):
    """
    Operator that fetches job postings from Jobicy.

    Arguments:
        conn_id - a connection id from the Apache Airflow metastore
        output_path - the place to put the JSON file with the results
    """

    template_fields = ("_start_date", "_output_path")

    @apply_defaults
    def __init__(
        self,
        conn_id,
        output_path,
        start_date="{{ ds }}",
        **kwargs
    ):
        super(JobicyGetJobsOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date

    # pylint: disable=unused-argument,missing-docstring
    def execute(self, context):
        hook = JobicyHook(self._conn_id)

        try:
            self.log.info("Starting to fetch the data from Jobicy")
            results = list(hook.get_job_offers())
            self.log.info("Fetching data from Jobicy successful")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            self.log.error(e)

        self.log.info(f"Writing offers to {self._output_path}")

        # Make sure output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Write output as JSON.
        with open(self._output_path, "w") as file_:
            json.dump(results, fp=file_)

        self.log.info('Results file size: %s', os.path.getsize(self._output_path))
