import os

def credentials():
    """
    mysql credentials
    """
    return{
        "username": os.getenv('MYSQL_USERNAME'),
        "password": os.getenv('MYSQL_PASSWORD'),
        "host": os.getenv('MYSQL_HOST'),
        "database": os.getenv('MYSQL_DATABASE')}