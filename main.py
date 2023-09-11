import Database_Create_and_Insert_films_table
import Database_Create_and_Insert_people_table
import Database_Create_and_Insert_reviews_table
import Database_Create_and_Insert_roles_table

def main():
    print("Starting main script...")
    
    #Execute the scripts for creating and populating tables
    print("Calling Database_Create_and_Insert_films_table...")
    Database_Create_and_Insert_films_table.main()
    print("Database_Create_and_Insert_films_table completed.")

    print("Calling Database_Create_and_Insert_people_table...")
    Database_Create_and_Insert_people_table.main()
    print("Database_Create_and_Insert_people_table completed.")
    
    print("Calling Database_Create_and_Insert_reviews_table...")
    Database_Create_and_Insert_reviews_table.main()
    print("Database_Create_and_Insert_reviews_table completed.")

    print("Calling Database_Create_and_Insert_roles_table...")
    Database_Create_and_Insert_roles_table.main()
    print("Database_Create_and_Insert_roles_table completed.")

    print("Main script completed.")

if __name__ == "__main__":
    main()


"""
This is the main function !
It executes every called function from the other .py files for the project.
After successful execution messages are printed and the process must end
with "Main script completed" message in the terminal.
All changes can be seen in the DB environment (DBeaver).
"""