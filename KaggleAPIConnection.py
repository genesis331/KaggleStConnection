from streamlit.connections import BaseConnection
from streamlit.runtime.caching import cache_data
import os
from zipfile import ZipFile
from io import StringIO
import pandas as pd
os.environ['KAGGLE_USERNAME'] = ""
os.environ['KAGGLE_KEY'] = ""
from kaggle.api.kaggle_api_extended import KaggleApi


class KaggleAPIConnection(BaseConnection[KaggleApi]):
    """st.connection implementation for Kaggle Public API"""

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

    def query(self, query: str, ttl: int = 3600, **kwargs) -> pd.DataFrame:
        """Executes a query and returns the result."""
        # Tell streamlit to not hash this parameter
        if 'file' in kwargs:
            kwargs['_file'] = kwargs.pop('file')
        # Cache the result
        @cache_data(ttl=ttl)
        # Query the Kaggle Public API
        def _query(query: str, **kwargs) -> pd.DataFrame:
            cursor = self.cursor()
            # Split the query into owner_slug, dataset_slug, dataset_version_number
            owner_slug, dataset_slug, dataset_version_number = cursor.split_dataset_string(query)
            # Get the dataset files
            ref_files = cursor.datasets_list_files(owner_slug, dataset_slug)['datasetFiles']
            # Check if "file" is one of the arguments
            file_index = 0
            if '_file' in kwargs:
                # Get the index of the file and set 0 if it doesn't exist
                file_name = kwargs.pop('_file')
                for x in ref_files:
                    if x['nameNullable'] == file_name:
                        file_index = ref_files.index(x)
            # Download the file
            output = cursor.datasets_download_file(owner_slug, dataset_slug, ref_files[file_index]['nameNullable'],
                                                   _preload_content=False, async_req=True, **kwargs)
            # Return the result as a Pandas DataFrame
            if output.get().info()['Content-Type'] == 'text/csv':
                return pd.read_csv(StringIO(output.get().read().decode('utf-8')))
            if output.get().info()['Content-Type'] == 'application/zip':
                # Check if temp folder exists and create it if it doesn't
                if not os.path.exists("temp/"):
                    os.makedirs("temp/")
                # Rename the file to .zip
                with open("temp/" + ref_files[file_index]['nameNullable'][:-4] + ".zip", 'wb') as f:
                    f.write(output.get().read())
                # Unzip the file
                with ZipFile("temp/" + ref_files[file_index]['nameNullable'][:-4] + ".zip", 'r') as zipObj:
                    zipObj.extractall("temp/")
                return pd.read_csv("temp/" + ref_files[file_index]['nameNullable'][:-4] + ".csv")
            raise Exception("File type not supported")
        return _query(query, **kwargs)
