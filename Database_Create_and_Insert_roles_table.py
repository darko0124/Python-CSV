import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine

def read_db_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config_lines = config_file.readlines()

    DB_USER = config_lines[0].strip()
    DB_PASSWORD = config_lines[1].strip()
    DB_HOST = config_lines[2].strip()
    DB_PORT = int(config_lines[3].strip())
    DB_NAME = config_lines[4].strip()

    return DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

""" 
Reads the credentials of the Database
from the configuration file and uses them for establishing a connection.
"""

def connect_to_database(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        print("Connected to the PostgreSQL database!")
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Error connecting to the PostgreSQL database: {error}")
        return None
    
""" 
Establishes a connection with the database. 
If there is a problem gives an error message.
"""
    
def create_schema(connection, schema_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    except psycopg2.Error as error:
        print(f"Error creating schema: {error}")


def create_table(connection, schema_name, table_name, column_names):
    try:
            cursor= connection.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ("
                   "id serial PRIMARY KEY,"
                   "film_id INTEGER,"
                   "person_id INTEGER,"
                   "role VARCHAR(255)"
                   ");")
            cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY;")
    except psycopg2.Error as error:
        print(f"Error creating table: {error}")


""" 
Creating the schema and the table in the DB.
"""

    
def insert_data_to_table(engine, schema_name, table_name, df):
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema_name)
    except Exception as error:
        print(f"Error inserting data into table: {error}")

"""
Inserting data into the table.
"""

def close_connection(connection, cursor):
    if connection:
        if cursor:
            cursor.close()  # Close the cursor
        connection.close()
        print("Database connection closed.")


def main():
    config_file_path = r'C:\VS Code Projects\Valq task\DB_Config.txt'
    DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = read_db_config(config_file_path)
    
    connection = connect_to_database(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    cursor = None  
    
    if connection:
        cursor = connection.cursor() 
        
        schema_name = "Darin_films"
        table_name = "roles"
        column_names = ["id", "film_id", "person_id", "role"]
        
        create_schema(connection, schema_name)
        create_table(connection, schema_name, table_name, column_names)
        
        df = pd.read_csv("roles.csv", names=column_names, header=0)
        
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        insert_data_to_table(engine, schema_name, table_name, df)
        
        connection.commit()
        
        print("Data inserted successfully!")
    
    close_connection(connection, cursor)  

if __name__ == "__main__":
    main()

"""
This is the main function !
Using the given parameters schema_name, table_name and column_names
main() creates the table in the schema, then using pd.read_csv
reads the given csv file and extracts it's data inside the table in
the DB.
"""