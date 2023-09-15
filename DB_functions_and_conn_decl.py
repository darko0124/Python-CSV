import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from typing import List

def read_db_config(config_file_path: str):
    """ 
    Reads the credentials of the Database
    from a specified configuration file and 
    uses them to establish a connection.

    Args:
        config_file_path (str): A string representation
        of the path to the configuration file

    Returns a tuple containig:
        DB_USER -> Username of user in DB
        DB_PASSWORD -> Password of the user for the DB
        DB_HOST -> Hostname of the DB server
        DB_PORT -> Port number used for the DB connection
        DB_NAME -> Name of the DB to connect to
    """
    with open(config_file_path, 'r') as config_file:
        config_lines = config_file.readlines()
    
    DB_USER = config_lines[0].strip()
    DB_PASSWORD = config_lines[1].strip()
    DB_HOST = config_lines[2].strip()
    DB_PORT = int(config_lines[3].strip())
    DB_NAME = config_lines[4].strip()
    
    return DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME



def connect_to_database(DB_USER:str, DB_PASSWORD:str, DB_HOST:str, DB_PORT:str, DB_NAME:str):
    """ 
    Establishes a connection with a PostgreSQL database. 
   
    Args:
        DB_USER (str): The username for the database.
        DB_PASSWORD (str): The password for the database user.
        DB_HOST (str): The hostname or IP address of the database server.
        DB_PORT (int): The port number to connect to on the database server.
        DB_NAME (str): The name of the database to connect to.

    Returns a message saying:
        psycopg2.extensions.connection: A connection object if successful.
        None: Returns None if there is an error.

    Raises:
        Message :"Connected to the PostgreSQL database!" -> if the connection is successful.
        An error message if there is a problem connecting to the database.
    """
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

def create_table(connection: psycopg2.extensions.connection, schema_name: str, table_name: str, column_names: List[str]):
    """ 
    Creates a new table in the PostgreSQL database.
    1. If the table already exists: truncates it and restarts it's identity.
    2. If it does not exist, it creates it.
    
   
    Args:
        connection (psycopg2.extensions.connection): A connection to the PostgreSQL database.
        schema_name (str): The name of the schema to be created.
        table_name (str): The name of the table to be created.
        column_names (str): The names of the columns in the created table.

    Returns:
        None

    Raises:
        psycopg2.Error: If there's an error while creating the table.
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ("");")
        cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name} RESTART IDENTITY;")
    except psycopg2.Error as error:
        print(f"Error creating table: {error}")

def insert_data_to_table(engine: create_engine, schema_name: str, table_name: str, df):
    """ 
    Inserts data from a dataframe into the DB.

    Args: 
        engine (str): SQLAlchemy engine to connect to the database. 
        schema_name (str): The name of the schema to be created.
        table_name (str): The name of the table to be created.
        df (DataFrame): The DataFrame containing the data to be inserted.

    Returns:
        None

    Raises:
        None
    """

    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema_name)
    except Exception as error:
        print(f"Error inserting data into table: {error}")

def close_connection(connection, cursor):
    """ 
    Closes the connection with the PostgreSQL database. 
   
    Args:
        connection: The database connection to be closed.
        cursor: The cursor associated with the connection.

    Returns:
        None
        
    Raises:
        None    
    """
    if connection:
        if cursor:
            cursor.close()  # Close the cursor
        connection.close()
        print("Database connection closed.")
