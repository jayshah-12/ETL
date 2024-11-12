"""
importing sql alchemy
"""
from sqlalchemy import create_engine
from database.base_database import BaseDatabase

class MysqlDatabase(BaseDatabase):
    """
    base class for databases
    """
    def __init__(self, credentials):
        self.username = credentials.get('username')
        self.password = credentials.get('password')
        self.host = credentials.get('host')
        self.database = credentials.get('database')
        self.engine = self.create_engine()

    def create_engine(self):
        """Create a SQLAlchemy engine for MySQL."""
        return create_engine(
            f"mysql+pymysql://{self.username}:{self.password}@{self.host}/{self.database}"
        )
