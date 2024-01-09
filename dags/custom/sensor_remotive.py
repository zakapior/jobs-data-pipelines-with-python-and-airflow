"""
Module containing Remotive jobs JSON file sensor. It detects, if there
were new job postings.
"""

from json import load
import pendulum

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class RemotiveNewJobsSensor(BaseSensorOperator):
    """
    Sensor that checks if there are new jobs in the Remotive JSON file.

    start_date : str
        (Templated) start date of the time period to check for (inclusive).
    end_date : str
        (Templated) end date of the time period to check for (exclusive).
    """

    template_fields = ("_start_date", "_end_date")
    INPUT_JOB_FILE = '/tmp/job_offers/data/remotive.json'

    @apply_defaults
    def __init__(self,
                 start_date="{{ data_interval_start }}",
                 end_date="{{ data_interval_end }}",
                 **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._end_date = end_date

    # pylint: disable=unused-argument,missing-docstring
    def poke(self, context):

        try:
            self.log.info('Start date: %s', self._start_date)
            self.log.info('End date: %s', self._end_date)
            with open('/tmp/job_offers/data/remotive.json', 'r') as input_file:
                jobs = load(input_file)
            self.log.info("Input file found")
        except FileNotFoundError:
            self.log.info(
                "File not found: /tmp/job_offers/data/remotive.json"
            )
            return False

        start_date = pendulum.parse(self._start_date)
        end_date = pendulum.parse(self._end_date)

        for job in jobs:
            date_published = pendulum.parse(job['publication_date'])
            if date_published >= start_date and date_published < end_date:
                self.log.info("New jobs found")
                return True
            else:
                self.log.info("No new jobs")
                return False
