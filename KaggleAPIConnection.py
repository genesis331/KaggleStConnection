from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import os
from io import StringIO
import pandas as pd
os.environ['KAGGLE_USERNAME'] = ""
os.environ['KAGGLE_KEY'] = ""
from kaggle.api.kaggle_api_extended import KaggleApi


class KaggleAPIConnection(ExperimentalBaseConnection[KaggleApi]):
    """st.experimental_connection implementation for Kaggle Public API"""

    def _connect(self, **kwargs) -> KaggleApi:
        """Connects to the Kaggle Public API and returns a cursor object."""
        # Load Kaggle API credentials from either arguments or secrets.toml file
        if 'kaggle_username' in kwargs:
            os.environ['KAGGLE_USERNAME'] = kwargs.pop('kaggle_username')
        else:
            os.environ['KAGGLE_USERNAME'] = self._secrets['kaggle_username']
        if 'kaggle_key' in kwargs:
            os.environ['KAGGLE_KEY'] = kwargs.pop('kaggle_key')
        else:
            os.environ['KAGGLE_KEY'] = self._secrets['kaggle_key']
        # Initialize Kaggle API
        api = KaggleApi()
        # Authenticate using environment variables
        api.authenticate()
        return api

    def cursor(self) -> KaggleApi:
        """Returns a cursor object."""
        return self._instance

    def query(self, query: str, ttl: int = 3600, **kwargs) -> any:
        """Executes a query and returns the result."""
        # Cache the result
        @cache_data(ttl=ttl)
        # Query the Kaggle Public API
        def _query(query: str, **kwargs) -> any:
            cursor = self.cursor()
            # Split the query into owner_slug, dataset_slug, dataset_version_number
            owner_slug, dataset_slug, dataset_version_number = cursor.split_dataset_string(query)
            # Get the dataset files
            ref_files = cursor.datasets_list_files(owner_slug, dataset_slug)['datasetFiles']
            # Download the first file
            output = cursor.datasets_download_file(owner_slug, dataset_slug, ref_files[0]['nameNullable'],
                                                   _preload_content=False, **kwargs)
            # Return the result as a Pandas DataFrame if the file is a CSV file
            if output.info()['Content-Type'] == 'text/csv':
                return pd.read_csv(StringIO(output.read().decode('utf-8')))
            # Raise an exception if the file is not a CSV file
            raise Exception('Not a CSV file')
        return _query(query, **kwargs)
