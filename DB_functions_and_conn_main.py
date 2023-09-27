import psycopg2
import os
from DB_functions_and_conn_decl import create_table, insert_data_to_table, read_db_config, connect_to_database, close_connection
import pandas as pd
from sqlalchemy import create_engine

def main():
    config_file_path = r'C:\VS Code Projects\Valq task\DB_Config.txt'
    DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = read_db_config(config_file_path)

    connection = connect_to_database(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    cursor = None

    if connection:
        cursor = connection.cursor()

        # Define parameters for the films table
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        schema_name = "Darin_films"
        dict_tables = {
            "films": ["id", "title", "release_year", "country", "duration", "language", "certification", "gross", "budget"],
            "people": ["id", "name", "birthdate", "deathdate"],
            "reviews": ["film_id", "num_user", "num_critic", "imdb_score", "num_votes", "facebook_likes"],
            "roles" :["id", "film_id", "person_id", "role"]
        }

        for table, columns in dict_tables.items():
            create_table(connection, schema_name, table, columns)
            df = pd.read_csv(f"C:\\VS Code Projects\\Valq task\\Valq_Task_Datasets\\{table}.csv") #Try to use the different path for the file (with //).

            insert_data_to_table(engine, schema_name, table, df)
            connection.commit()
            print(f"Data inserted successfully for table {table}!")


    close_connection(connection, cursor)

if __name__ == "__main__":
    main()