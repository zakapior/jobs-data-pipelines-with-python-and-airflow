
"""
This is a module with the Jobicy hook implementation for Apache Airflow.
"""
from feedparser import parse
from airflow.hooks.base import BaseHook


class JobicyHook(BaseHook):
    """
    This is the hook for accessing the Jobicy job postings.

    Arguments:
        conn_id - a connection id from the Apache Airflow metastore
    """

    DEFAULT_SCHEMA = "https"

    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id

        self._base_url = None

    def __enter__(self):
        return self

    def get_conn(self):
        """
        Creates the connection used by the hook for querying data. Uses the
        connection data from the metastore.

        Arguments:
            none
        
        Returns:
            _base_url - the base URL of the website
        """

        config = self.get_connection(self._conn_id)

        if not config.host:
            raise ValueError(f"No host specified in {self._conn_id}")

        schema = config.schema or self.DEFAULT_SCHEMA

        self._base_url = f"{schema}://{config.host}"

        return self._base_url

    def get_job_offers(self):
        """
        Returns a generator for the jobs at Jobicy.

        Returns:
            feed.entries - generator with the job postings at Jobicy as a
            JSON structure
        """
        self.get_conn()
        endpoint = '/?feed=job_feed'
        url = self._base_url + endpoint

        try:
            feed = parse(url)
        # pylint: disable=broad-exception-caught
        except Exception as e:
            self.log.error(e)

        yield from feed.entries
