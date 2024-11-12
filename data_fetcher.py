import json
import importlib


class DataFetcher:
    """
    Base class to initialise database, load api config and execute the fetch function
    """
    def __init__(self, api_key, db_credentials, provider_name, db_type):
        self.api_key = api_key
        self.provider_name = provider_name
        self.failed_offsets = []
        self.api_calls = self.load_api_calls(provider_name)
        self.database = self.load_database_handler(db_type, db_credentials)
        self.credentials = db_credentials

    def load_api_calls(self, provider_name):
        """
        load config from 
        """
        config_file = f"providers/{provider_name}.json"
        #loading config file of required provider
        print(config_file)
        with open(config_file, encoding="utf-8") as f:
            config = json.load(f)
        return config['api_calls']



    def load_database_handler(self, db_type, db_credentials):
        db_select = None
        #initializing object of the req database
        db_module = importlib.import_module(f"database.{db_type}")
        class_name = f"{db_type.capitalize()}Database"
        db_class = getattr(db_module, class_name)
        db_select = db_class(db_credentials)
        print(db_select)

        return db_select

    def run(self):
        """
        EXECUTE THE PYTHON SCRIPT
        """
        provider_module = importlib.import_module(f"Fetch.{self.provider_name}")
        # fetch_data = getattr(provider_module, 'fetch_data')
        for api_call in self.api_calls:
            provider_module.fetch_data(self, api_call)
