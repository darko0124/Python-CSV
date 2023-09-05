import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine

# Read database configuration from DB_Config.txt file
with open(r'C:\VS Code Projects\Valq task\DB_Config.txt', 'r') as config_file:
    config_lines = config_file.readlines()

# Extract the configuration values
DB_USER = config_lines[0].strip()
DB_PASSWORD = config_lines[1].strip()
DB_HOST = config_lines[2].strip()
DB_PORT = int(config_lines[3].strip())
DB_NAME = config_lines[4].strip()

# Initialize the connection variable outside the try block
connection = None

try:
    # Attempt to connect to the database using psycopg2
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )

    # If the connection is successful, you can print a message
    print("Connected to the PostgreSQL database!")

    # Create a cursor to interact with the database
    cursor = connection.cursor()

    # Use SQLAlchemy to create an engine
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Specify the schema name and table name
    schema_name = "Darin_films"
    table_name = "people"

    # Read data from the CSV file
    df = pd.read_csv("people.csv")

    # Specify the schema name in the table creation and use if_exists="append" to add data
    df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema_name)

    # Commit the changes to the database using psycopg2
    connection.commit()

    # Print a message after a successful commit
    print("Data inserted successfully!")

except (Exception, psycopg2.Error) as error:
    print(f"Error connecting to the PostgreSQL database: {error}")
finally:
    # Close the database connection when done
    if connection:
        cursor.close()
        connection.close()
        print("Database connection closed.")
