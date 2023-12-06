# KaggleStConnection Documentation

**KaggleStConnection** is a Streamlit connection class that connects to Kaggle API. Better than the default [Kaggle API](https://www.kaggle.com/docs/api), **KaggleStConnection** automates the reading of a specific data file from your desired dataset found on Kaggle.

You can view the source GitHub repository for the connection class and the demo app in the links below:

* [KaggleStConnection](https://github.com/genesis331/KaggleStConnection)
* [KaggleStConnectionDemo](https://github.com/LimJY03/KaggleStConnectionDemo)

You may access the app at [this link](https://kagglestconnection.streamlit.app/).

### Package Installation

To install **KaggleStConnection** in your local machine, install the package with the code below in the terminal and you are ready to go.

```sh
pip install kaggle-st-connection
```

### Initializing KaggleStConnection

After you have installed the **KaggleStConnection** package in the step above, you can start to initialize a connection to the Kaggle API and obtain a connection object with the following code.

```py
# Package imports
from KaggleAPIConnection import KaggleAPIConnection
import streamlit as st

# Initialize connection
conn = st.connection("kaggle", type=KaggleAPIConnection)

# Obtain connection object
cursor = conn.cursor()
```

You can use the connection object `cursor` to call some KaggleAPI methods like `dataset_list()` and many more depending on your use cases.

### Querying Data From Kaggle Datasets

We can use the `query()` method and specify the Kaggle dataset reference in the format of `'[owner_slug]/[dataset_slug]'` as the method argument.

We can additionally use the `file` keyword argument to specify the dataset file to query from. By default, the first file in the dataset will be queried.

```py
# Using the previous initialized connection object `conn`
data = conn.query(
    'kritikseth/us-airbnb-open-data',   # Dataset ref
    file='AB_US_2023.csv'               # Optional file to open
)
```

In this case, we set the parameter `file` to `'AB_US_2023.csv'` so that we will obtain the desired latest data for our sample visualization shown below.

### Configuring Kaggle API Key

Before the above code can work, we will need to specify the API key during the initialization of the connection. The following quote the steps to obtain your API key on [kaggle.com](https://www.kaggle.com/) from the Kaggle Documentation.

> **🗨 Kaggle's Public API Documentation ([Read Here](https://www.kaggle.com/docs/api))**
> <br>In order to use Kaggle's public API, you must first authenticate using an API token. Go to the 'Account' tab of your user profile and select 'Create New Token'. This will trigger the download of `kaggle.json`, a file containing your API credentials.

To use your API keys in Streamlit, you can either:

* Go to your **Project Root (~)** and create a `.kaggle` folder and place your `kaggle.json` inside the folder.
* Go to your **Workspace Root** and create a `.streamlit` folder and create a `secrets.toml` file, then enter the following in the file:
  ```toml
  [connections.kaggle]
  kaggle_username='<USERNAME>'
  kaggle_key='<API_KEY>'
  ```
  Replace the `<USERNAME>` and the `<API_KEY>` with that in your `kaggle.json` file.
* **[NOT RECOMMENDED FOR PUBLIC DEPLOYMENT]** Specify the `kaggle_username` and `kaggle_key` optional parameter during the initialization of the connection with `st.experimental_connection`. An example of this use case is shown below:
  ```py
  conn = st.connection(
      'kaggle', 
      type=KaggleAPIConnection,
      kaggle_username='<USERNAME>',
      kaggle_key='<API_KEY>'
  )
  ```
  Similarly to the previous bullet, replace  the `<USERNAME>` and the `<API_KEY>` with that in your `kaggle.json` file.

You can read more about secrets management in [Streamlit's secrets management documentation](https://docs.streamlit.io/streamlit-community-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management).
