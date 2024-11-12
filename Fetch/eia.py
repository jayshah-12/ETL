"""
Required libraries
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error


def get_last_offset(table_name, mysql_credentials):
    """
    Fetch the last offset from MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host=mysql_credentials['host'],
            database=mysql_credentials['database'],
            user=mysql_credentials['username'],
            password=mysql_credentials['password']
        )
        cursor = connection.cursor()
        query = "SELECT max(last_offset) FROM api_fetch_offsets WHERE table_name = %s"
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return result[0]
        else:
            return 0
    except Error as e:
        print(f"Error fetching last offset: {str(e)}")
        return 0
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_last_offset(table_name, offset, mysql_credentials):
    """
    Update the last offset in MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host=mysql_credentials['host'],
            database=mysql_credentials['database'],
            user=mysql_credentials['username'],
            password=mysql_credentials['password']
        )
        cursor = connection.cursor()
        delete_query = "DELETE FROM api_fetch_offsets WHERE table_name = %s"
        cursor.execute(delete_query, (table_name,))
        query = "INSERT INTO api_fetch_offsets (table_name, last_offset) VALUES (%s, %s)"
        cursor.execute(query,(table_name,offset))
        connection.commit()
        print(f"Updated last fetched offset for {table_name} to {offset}.")
    except Error as e:
        print(f"Error updating last offset: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def fetch_data(self, api_call):
    """
    Function to fetch data from eia
    """
    url = f"https://api.eia.gov/v2/{api_call['url']}"
    params = api_call['params']
    params['api_key'] = self.api_key
    db_lock = threading.Lock()
    print(self.credentials)
    last_offset = get_last_offset(api_call['table_name'], self.credentials) 
    try:
        response = requests.get(url, params=params,timeout=100)
        response.raise_for_status()
        json_data = response.json()
        total_records = int(json_data['response']['total'])

        with ThreadPoolExecutor(max_workers=5) as executor:
            offset_map = {}
            for offset in range(last_offset, total_records, 5000):
                temp_params = params.copy()
                temp_params['offset'] = offset
                future = executor.submit(requests.get, url, temp_params)
                offset_map[future] = offset

            for future in as_completed(offset_map):
                offset = offset_map[future]
                try:
                    response = future.result()
                    response.raise_for_status()
                    data = response.json()

                    if 'data' in data['response']:
                        df = pd.DataFrame(data['response']['data'])
                        if not df.empty:
                            if 'columns' in api_call:
                                df = df[api_call['columns']]
                            df = df.dropna(subset=['value'])
                            df['value'] = pd.to_numeric(df['value'], errors='coerce')
                            update_last_offset(api_call['table_name'], offset, self.credentials)
                            with db_lock:
                                self.database.insert_data(df, api_call['table_name'])
                                print(f"data inserted at offset {offset}")
                except requests.exceptions.HTTPError:
                    self.failed_offsets.append(offset)

    except Exception as e:
        print(f"Error fetching data for {api_call['table_name']}: {e}")
        self.failed_offsets.append(offset)