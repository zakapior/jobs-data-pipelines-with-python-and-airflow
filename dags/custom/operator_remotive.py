"""
This is a module with the Remotive operator implementation for Apache Airflow.
"""
import json
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from custom.hook_remotive import RemotiveHook


class RemotiveGetJobsOperator(BaseOperator):
    """
    Operator that fetches job postings from Remotive.

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
        **kwargs
    ):
        super(RemotiveGetJobsOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._output_path = output_path

    # pylint: disable=unused-argument,missing-docstring
    def execute(self, context):
        hook = RemotiveHook(self._conn_id)

        try:
            self.log.info("Starting to fetch the data from Remotive")
            results = list(hook.get_job_offers())
            self.log.info("Fetching data from Remotive successful")
        finally:
            hook.close()

        self.log.info(f"Writing offers to {self._output_path}")

        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self._output_path, "w") as file_:
            json.dump(results, fp=file_)

        self.log.info('Results file size: %s', os.path.getsize(self._output_path))
