class BaseDatabase:
    """
    Base class for different databases
    """
    def insert_data(self, df, table_name):
        """Insert data into the specified table."""    
        df.to_sql(table_name, self.engine, if_exists='append', index=False)
        print(f"Inserted {len(df)} records into {table_name}.")