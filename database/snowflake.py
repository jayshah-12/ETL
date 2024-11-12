from sqlalchemy import create_engine
from database.base_database import BaseDatabase

class SnowflakeDatabase(BaseDatabase):
    def __init__(self, credentials):
        self.username = credentials.get('user')
        self.password = credentials.get('password')
        self.account = credentials.get('account')
        self.database = credentials.get('database')
        self.schema = credentials.get('schema')
        self.engine = self.create_engine(credentials)

    def create_engine(self, credentials):
        return create_engine(
            f"snowflake://{self.username}:{self.password}@{self.account}/{self.database}/{self.schema}"
        )
