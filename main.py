"""
REQUIRED LIBRARIES
"""
import os
import importlib
from dotenv import load_dotenv

from data_fetcher import DataFetcher

load_dotenv()

PROVIDER_NAME = 'eia'
DB_TYPE = 'mysql'

api_key = os.getenv(f"{PROVIDER_NAME}_API_KEY")

db_credentials_module = importlib.import_module(f"db_credentials.{DB_TYPE}_credentials")
db_credentials = db_credentials_module.credentials()

data_fetcher = DataFetcher(api_key, db_credentials, PROVIDER_NAME, DB_TYPE)
data_fetcher.run()
