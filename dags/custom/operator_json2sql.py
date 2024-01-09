"""
This is a module that implements a JSON to SQL file operator Apache
Airflow.
"""
import json
from dateutil.parser import parse

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class Json2SqlOperator(BaseOperator):
    """
    Operator transform the job postings from JSON to SQL file.

    Arguments:
        service_name - the name of the service we want to get data from.
            Can be either 'remotive', 'weworkremotely' or 'jobicy'.
        start_date - data interval begginig
        end_date - data interval end
    """

    template_fields = ("_start_date", "_end_date")
    working_directory = "/tmp/job_offers/data/"
    fields_mapping = {
        'remotive': [
            'id',
            'title',
            'company_name',
            'url',
            'job_type',
            'candidate_required_location',
            'salary',
            'publication_date'
        ],
        'weworkremotely': [
            'id',
            'title',
            '',
            'link',
            'type',
            'region',
            '',
            'published'
        ],
        'jobicy': [
            'id',
            'title',
            'job_listing_company',
            'link',
            'job_listing_job_type',
            'job_listing_location',
            '',
            'published'
        ]
    }

    @apply_defaults
    def __init__(
        self,
        service_name,
        start_date="{{ data_interval_start }}",
        end_date = "{{ data_interval_end }}",
        **kwargs
    ):
        super().__init__(**kwargs)

        self._start_date = start_date
        self._end_date = end_date
        self.service_name = service_name

    # pylint: disable=unused-argument,missing-docstring
    def execute(self, context):
        try:
            # pylint: disable=unspecified-encoding
            with open(f'{self.working_directory}{self.service_name}.json',
                      'r') as input_file:
                jobs = json.load(input_file)
            self.log.info("Input file found")
        except FileNotFoundError:
            self.log.info(
                f"File not found: {self.working_directory}{self.service_name}.json"
            )
            return False

        start_date = parse(self._start_date, fuzzy=True, ignoretz=True)
        end_date = parse(self._end_date, fuzzy=True, ignoretz=True)
        query = str()

        for job in jobs:
            publication_date = parse(job[self.fields_mapping[self.service_name][7]], fuzzy=True, ignoretz=True)

            if publication_date >= start_date and publication_date < end_date:
                query = query + f"INSERT INTO jobs VALUES ( '{self.service_name}', "
                for i, field in enumerate(self.fields_mapping[self.service_name]):
                    if i == 7:
                        query = query + "'" + f"{parse(job[field], fuzzy=True, ignoretz=True)}" + "'"
                    elif field == '' or field not in job.keys():
                        query = query + "'" + "', "
                    else:
                        query = query + "'" + f"{job[field]}".replace("\'", "") + "', "

                query += ' );\n'

        # pylint: disable=unspecified-encoding
        with open(f'{self.working_directory}{self.service_name}-'
                  '{self._start_date}.sql', 'w') as sql_file:
            sql_file.writelines(query)
