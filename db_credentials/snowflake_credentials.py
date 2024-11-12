import os

def credentials():
    """
    Snowflake credentials
    """
    return {
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "database": os.getenv('SNOWFLAKE_DATABASE'),
        "schema": os.getenv('SNOWFLAKE_SCHEMA')
    }
