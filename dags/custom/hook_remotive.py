"""
This is a module with the Jobicy hook implementation for Apache Airflow.
"""
import requests

from airflow.hooks.base import BaseHook


class RemotiveHook(BaseHook):
    """
    This is the hook for accessing the Remotive job postings.

    Arguments:
        conn_id - a connection id from the Apache Airflow metastore
    """

    DEFAULT_SCHEMA = "https"

    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id

        self._session = None
        self._base_url = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self):
        """
        Creates the connection used by the hook for querying data. Uses the
        connection data from the metastore.

        Arguments:
            none
        
        Returns:
            _session - the session object for future usage
            _base_url - the base URL of the website
        """

        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in {self._conn_id}")

            schema = config.schema or self.DEFAULT_SCHEMA

            self._base_url = f"{schema}://{config.host}"

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = requests.Session()

        return self._session, self._base_url

    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

    def get_job_offers(self):
        """
        Returns a generator for the jobs at Remotive.

        Returns:
            jobs - generator with the job postings at Remotive as a
            JSON structure
        """

        self.get_conn()
        endpoint = '/api/remote-jobs'
        url = self._base_url + endpoint

        try:
            response = self._session.get(url)
        # pylint: disable=broad-exception-caught
        except Exception as e:
            self.log.error(e)

        response.raise_for_status()

        yield from response.json()["jobs"]
