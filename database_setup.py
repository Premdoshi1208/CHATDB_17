import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from pymongo import MongoClient
import os
import numpy as np
import json

class DatabaseImporter:
    @staticmethod
    def get_mysql_type(dtype):
        """Convert pandas dtype to MySQL data type"""
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DECIMAL(10,2)"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        else:
            return "VARCHAR(255)"

    @staticmethod
    def create_mysql_database(host, user, password, database_name):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
            cursor = conn.cursor()

            # Create database
            cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Database '{database_name}' created successfully")

            # Create user and grant privileges
            cursor.execute(f"CREATE USER IF NOT EXISTS 'chatdb_user'@'localhost' IDENTIFIED BY 'your_password'")
            cursor.execute(f"GRANT ALL PRIVILEGES ON {database_name}.* TO 'chatdb_user'@'localhost'")
            cursor.execute("FLUSH PRIVILEGES")
            print("User 'chatdb_user' created and privileges granted")

            conn.close()
            return True
        except Exception as e:
            print(f"Error creating MySQL database: {e}")
            return False

    @staticmethod
    def import_csv_to_mysql(host, user, password, database_name, csv_file, table_name):
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Create MySQL connection
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database_name
            )
            cursor = conn.cursor()

            # Dynamically create table schema based on DataFrame columns and data types
            columns = []
            for column, dtype in df.dtypes.items():
                mysql_type = DatabaseImporter.get_mysql_type(dtype)
                columns.append(f"`{column}` {mysql_type}")

            create_table_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
            """
            cursor.execute(create_table_query)
            print(f"Created table with schema:\n{create_table_query}")
            
            # Prepare the insert query
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples
            values = [tuple(x) for x in df.replace({np.nan: None}).values]
            
            # Insert in batches
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()
                print(f"Inserted records {i} to {min(i + batch_size, len(values))}")
            
            conn.close()
            print(f"Data imported successfully to MySQL table '{table_name}'")
            return True
        except Exception as e:
            print(f"Error importing data to MySQL: {e}")
            return False

    @staticmethod
    def import_csv_to_mongodb(connection_string, database_name, csv_file, collection_name):
        try:
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]

            # Read CSV file
            df = pd.read_csv(csv_file)

            # Handle datetime columns
            for column in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Convert DataFrame to list of dictionaries
            records = df.replace({np.nan: None}).to_dict('records')

            # Drop existing collection if it exists
            db[collection_name].drop()

            # Insert data
            collection = db[collection_name]
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)
                print(f"Inserted records {i} to {min(i + batch_size, len(records))}")

            print(f"Data imported successfully to MongoDB collection '{collection_name}'")
            return True
        except Exception as e:
            print(f"Error importing data to MongoDB: {e}")
            return False

    @staticmethod
    def import_json_to_mongodb(connection_string, database_name, json_file, collection_name):
        try:
            # Connect to MongoDB
            client = MongoClient(connection_string)
            db = client[database_name]
            
            # Initialize records list
            records = []
            
            # Try different JSON formats
            with open(json_file) as f:
                try:
                    # First try: Read as single JSON array or object
                    content = f.read()
                    try:
                        # Try parsing as JSON array/object
                        data = json.loads(content)
                        if isinstance(data, list):
                            records = data
                        elif isinstance(data, dict):
                            # Check if it's a nested structure with an array
                            for value in data.values():
                                if isinstance(value, list):
                                    records = value
                                    break
                            if not records:  # If no array found, treat as single record
                                records = [data]
                        else:
                            records = [data]
                    except json.JSONDecodeError:
                        # Try parsing as JSON Lines
                        records = []
                        for line in content.splitlines():
                            line = line.strip()
                            if line:  # Skip empty lines
                                try:
                                    record = json.loads(line)
                                    if record:  # Skip empty objects
                                        records.append(record)
                                except json.JSONDecodeError as e:
                                    print(f"Warning: Skipping invalid JSON line: {line[:100]}...")
                                    continue
                except Exception as e:
                    print(f"Error reading JSON content: {e}")
                    return False

            if not records:
                print("No valid records found in file")
                return False

            print(f"\nSuccessfully parsed JSON data:")
            print(f"Number of records found: {len(records)}")
            print("\nSample record structure:")
            for key, value in records[0].items():
                print(f"- {key}: {type(value).__name__}")

            # Drop existing collection
            db[collection_name].drop()

            # Insert data
            collection = db[collection_name]
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)
                print(f"Inserted records {i} to {min(i + batch_size, len(records))}")

            print(f"\nData imported successfully to MongoDB collection '{collection_name}'")
            
            # Print some sample queries that can be used
            print("\nYou can now use queries like:")
            print("- show all data")
            print(f"- find records where {list(records[0].keys())[0]} equals <value>")
            print(f"- sort by {list(records[0].keys())[0]} descending")
            print(f"- show average {next((k for k, v in records[0].items() if isinstance(v, (int, float))), 'numeric_field')}")
            
            return True
        except Exception as e:
            print(f"Error importing data to MongoDB: {e}")
            print("\nPlease ensure your JSON file is in one of these formats:")
            print("1. JSON array: [{'field': 'value'}, ...]")
            print("2. JSON Lines: {'field': 'value'} (one object per line)")
            print("3. Single JSON object: {'field': 'value'}")
            print("4. Nested JSON with array: {'data': [{'field': 'value'}, ...]}")
            return False

def validate_csv_file(csv_file):
    """Validate if the CSV file exists and can be read"""
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found")
        return False
    
    try:
        df = pd.read_csv(csv_file)
        print(f"\nCSV file structure:")
        print("\nColumns found:")
        for column, dtype in df.dtypes.items():
            print(f"- {column}: {dtype}")
        print(f"\nTotal rows: {len(df)}")
        return True
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return False

def validate_json_file(json_file):
    """Validate if the JSON file exists and can be read"""
    if not os.path.exists(json_file):
        print(f"Error: File '{json_file}' not found")
        return False
    
    try:
        records = []
        with open(json_file) as f:
            content = f.read()
            try:
                # Try parsing as JSON array/object
                data = json.loads(content)
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    # Check if it's a nested structure with an array
                    for value in data.values():
                        if isinstance(value, list):
                            records = value
                            break
                    if not records:  # If no array found, treat as single record
                        records = [data]
                else:
                    records = [data]
            except json.JSONDecodeError:
                # Try parsing as JSON Lines
                records = []
                for line in content.splitlines():
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            records.append(record)
                        except json.JSONDecodeError:
                            continue

        if not records:
            print("No valid records found in file")
            return False

        sample = records[0]
        print(f"\nJSON file structure:")
        print("\nFields found:")
        for key, value in sample.items():
            print(f"- {key}: {type(value).__name__}")
        print(f"\nTotal records: {len(records)}")
        return True
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        print("\nPlease ensure your JSON file is in one of these formats:")
        print("1. JSON array: [{'field': 'value'}, ...]")
        print("2. JSON Lines: {'field': 'value'} (one object per line)")
        print("3. Single JSON object: {'field': 'value'}")
        print("4. Nested JSON with array: {'data': [{'field': 'value'}, ...]}")
        return False

def setup_database():
    print("\n--- Database Setup Utility ---")
    while True:
        print("\n1. Setup MySQL Database")
        print("2. Setup MongoDB Database")
        print("3. Exit")

        choice = input("\nEnter your choice (1-3): ")

        if choice == "1":
            print("\n--- MySQL Database Setup ---")
            host = input("Enter MySQL host (default: localhost): ") or "localhost"
            user = input("Enter MySQL root user (default: root): ") or "root"
            password = input("Enter MySQL root password: ")
            database_name = input("Enter new database name: ")
            csv_file = input("Enter path to CSV file: ")
            table_name = input("Enter table name for the data: ")

            if not validate_csv_file(csv_file):
                continue

            if DatabaseImporter.create_mysql_database(host, user, password, database_name):
                DatabaseImporter.import_csv_to_mysql(host, user, password, database_name, csv_file, table_name)
                print("\nMySQL setup completed!")
                print(f"You can now connect to the database using:")
                print(f"Host: {host}")
                print(f"User: chatdb_user")
                print(f"Password: your_password")
                print(f"Database: {database_name}")

        elif choice == "2":
            print("\n--- MongoDB Database Setup ---")
            connection_string = input("Enter MongoDB connection string (default: mongodb://localhost:27017/): ") or "mongodb://localhost:27017/"
            database_name = input("Enter database name: ")
            file_type = input("Enter file type (csv/json): ").lower()
            file_path = input("Enter path to file: ")
            collection_name = input("Enter collection name for the data: ")

            if file_type == 'csv':
                if not validate_csv_file(file_path):
                    continue
                if DatabaseImporter.import_csv_to_mongodb(connection_string, database_name, file_path, collection_name):
                    print("\nMongoDB setup completed!")
                    print(f"You can now connect to the database using:")
                    print(f"Connection string: {connection_string}")
                    print(f"Database: {database_name}")
            elif file_type == 'json':
                if not validate_json_file(file_path):
                    continue
                if DatabaseImporter.import_json_to_mongodb(connection_string, database_name, file_path, collection_name):
                    print("\nMongoDB setup completed!")
                    print(f"You can now connect to the database using:")
                    print(f"Connection string: {connection_string}")
                    print(f"Database: {database_name}")
            else:
                print("Invalid file type. Please choose 'csv' or 'json'")

        elif choice == "3":
            print("Exiting database setup utility...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    setup_database()
