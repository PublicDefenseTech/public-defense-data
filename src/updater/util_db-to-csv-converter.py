import psycopg2
import csv
import zipfile
import os
from dotenv import load_dotenv

def export_tables_to_csv(database_url, zip_filename):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()

    # Get the list of table names
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()

    # Temporary folder to store CSVs
    temp_dir = "temp_csvs"
    os.makedirs(temp_dir, exist_ok=True)

    # Export each table to a CSV file
    for table in tables:
        table_name = table[0]
        csv_file = os.path.join(temp_dir, f"{table_name}.csv")
        
        # Query the table data
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Write the rows to a CSV file
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Get column names (header)
            column_names = [desc[0] for desc in cursor.description]
            writer.writerow(column_names)
            
            # Write the data
            writer.writerows(rows)

    output_dir = "data"  # The directory where you want to save the zip
    os.makedirs(output_dir, exist_ok=True) # Create the directory if it doesn't exist
    output_path = os.path.join(output_dir, zip_filename) # Combine directory and filename

    # Create a ZIP file containing all the CSVs
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in os.listdir(temp_dir):
            zipf.write(os.path.join(temp_dir, file), file)

    # Clean up the temporary CSV folder
    for file in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, file))
    os.rmdir(temp_dir)

    # Close the database connection
    cursor.close()
    conn.close()

def load_db_env(file_path='src/updater/.env'):
    #Create a local environment field called 'env.env' with your credentials
    env_path = os.path.abspath(file_path)
    load_dotenv(env_path)
    DB_PARAMS = {
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
    }
    return DB_PARAMS

DB_PARAMS = load_db_env()
# Example usage:
user = DB_PARAMS['user']
password = DB_PARAMS['password']
dbname = DB_PARAMS['dbname']
database_url = f"postgresql://{user}:{password}@localhost/{dbname}"  # Replace with your database URL
zip_filename = "database_tables.zip"
export_tables_to_csv(database_url, zip_filename)
print(f"Database tables have been exported and compressed into {zip_filename}")
