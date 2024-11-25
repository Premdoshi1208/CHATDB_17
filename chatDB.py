import mysql.connector
from pymongo import MongoClient
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
from tabulate import tabulate
import pandas as pd

# Download necessary NLTK data
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True)

class ChatDB:
    def __init__(self):
        self.sql_db = None
        self.sql_cursor = None
        self.nosql_client = None
        self.nosql_db = None
        self.current_db = None
        self.current_db_type = None
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))

    def connect_sql(self, host, user, password, database):
        try:
            self.sql_db = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.sql_cursor = self.sql_db.cursor(dictionary=True)
            self.current_db = database
            self.current_db_type = "sql"
            return True
        except mysql.connector.Error as err:
            print(f"Error connecting to MySQL Database: {err}")
            return False

    def connect_nosql(self, connection_string, database):
        try:
            self.nosql_client = MongoClient(connection_string)
            self.nosql_db = self.nosql_client[database]
            self.current_db = database
            self.current_db_type = "nosql"
            return True
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return False

    def get_tables(self):
        if self.current_db_type == "sql":
            self.sql_cursor.execute("SHOW TABLES")
            tables = self.sql_cursor.fetchall()
            column_name = f'Tables_in_{self.current_db}'
            return [table[column_name] for table in tables]
        elif self.current_db_type == "nosql":
            return self.nosql_db.list_collection_names()
        return []

    def get_sample_data(self, table_name, limit=5):
        if self.current_db_type == "sql":
            try:
                self.sql_cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
                return self.sql_cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"Error fetching sample data: {err}")
                return None
        elif self.current_db_type == "nosql":
            try:
                return list(self.nosql_db[table_name].find().limit(limit))
            except Exception as e:
                print(f"Error fetching sample data: {e}")
                return None

    def execute_query(self, table_name, query):
        if self.current_db_type == "sql":
            try:
                self.sql_cursor.execute(query)
                return self.sql_cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"Error executing SQL query: {err}")
                return None
        elif self.current_db_type == "nosql":
            try:
                if isinstance(query, str):
                    # Convert string representation to actual MongoDB query
                    if query == "show all data":
                        pipeline = []  # Empty pipeline returns all documents
                    else:
                        try:
                            pipeline = eval(query)
                        except:
                            pipeline = [{'$match': {}}]  # Default to return all documents
                else:
                    pipeline = query
                
                # Execute MongoDB aggregation
                result = list(self.nosql_db[table_name].aggregate(pipeline))
                return result
            except Exception as e:
                print(f"Error executing MongoDB query: {e}")
                return None

    def process_natural_language_query(self, table_name, nl_query):
        try:
            nl_query = nl_query.lower().strip()
            
            if self.current_db_type == "nosql":
                # MongoDB specific queries
                if 'show all' in nl_query or 'show me all' in nl_query:
                    return list(self.nosql_db[table_name].find({}))
                    
                elif 'count' in nl_query:
                    return list(self.nosql_db[table_name].aggregate([
                        {'$count': 'total_records'}
                    ]))
                    
                elif 'greater than' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('where') + 1
                    value_index = words.index('greater') + 2
                    field = words[field_index]
                    value = float(words[value_index])
                    return list(self.nosql_db[table_name].find({field: {'$gt': value}}))
                    
                elif 'less than' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('where') + 1
                    value_index = words.index('less') + 2
                    field = words[field_index]
                    value = float(words[value_index])
                    return list(self.nosql_db[table_name].find({field: {'$lt': value}}))
                    
                elif 'average' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('average') + 1
                    field = words[field_index]
                    return list(self.nosql_db[table_name].aggregate([
                        {'$group': {'_id': None, 'average': {'$avg': f'${field}'}}}
                    ]))
                    
                elif 'sort by' in nl_query or 'order by' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('by') + 1
                    field = words[field_index]
                    direction = -1 if 'descending' in nl_query else 1
                    return list(self.nosql_db[table_name].find().sort(field, direction))
                
                else:
                    print("Query not recognized. Try these examples:")
                    print("- show all data")
                    print("- count all records")
                    print("- find transactions where unit_price is greater than 10")
                    print("- sort by unit_price descending")
                    return None
                    
            else:
                # SQL specific queries
                if 'greater than' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('where') + 1
                    value_index = words.index('greater') + 2
                    field = words[field_index]
                    value = words[value_index]
                    query = f"SELECT * FROM {table_name} WHERE {field} > {value}"
                    return self.execute_query(table_name, query)
                    
                elif 'less than' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('where') + 1
                    value_index = words.index('less') + 2
                    field = words[field_index]
                    value = words[value_index]
                    query = f"SELECT * FROM {table_name} WHERE {field} < {value}"
                    return self.execute_query(table_name, query)
                    
                elif 'show all' in nl_query or 'show me all' in nl_query:
                    query = f"SELECT * FROM {table_name}"
                    return self.execute_query(table_name, query)
                    
                elif 'count' in nl_query:
                    query = f"SELECT COUNT(*) as count FROM {table_name}"
                    return self.execute_query(table_name, query)
                    
                elif 'average' in nl_query:
                    words = nl_query.split()
                    field_index = words.index('average') + 1
                    field = words[field_index]
                    query = f"SELECT AVG({field}) as average FROM {table_name}"
                    return self.execute_query(table_name, query)
                    
                elif 'sort by' in nl_query or 'order by' in nl_query:
                    words = nl_query.split()
                    if 'sort by' in nl_query:
                        field_index = words.index('by') + 1
                    else:
                        field_index = words.index('order') + 2
                    field = words[field_index]
                    order = "DESC" if "descending" in nl_query else "ASC"
                    query = f"SELECT * FROM {table_name} ORDER BY {field} {order}"
                    return self.execute_query(table_name, query)
                
                else:
                    print("Query not recognized. Try these examples:")
                    print("- Show me all data")
                    print("- Find transactions where unit_price is greater than 10")
                    print("- Count all records")
                    print("- Show average unit_price")
                    print("- Sort by unit_price descending")
                    return None
                
        except Exception as e:
            print(f"Error processing query: {e}")
            print("Please try rephrasing your query.")
            return None

    def execute_custom_query(self, query):
        try:
            if self.current_db_type == "sql":
                return self.execute_query(None, query)
            elif self.current_db_type == "nosql":
                # For MongoDB, execute against the current database
                pipeline = eval(query)
                return list(self.nosql_db.command('aggregate', self.current_db, pipeline=pipeline, cursor={}))
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def visualize_data(self, data, chart_type='bar'):
        if not data:
            print("No data to visualize")
            return

        try:
            df = pd.DataFrame(data)
            
            if chart_type == 'bar':
                df.plot(kind='bar')
            elif chart_type == 'line':
                df.plot(kind='line')
            elif chart_type == 'scatter':
                if len(df.columns) < 2:
                    print("Scatter plot requires at least two columns of data")
                    return
                df.plot(kind='scatter', x=df.columns[0], y=df.columns[1])
            elif chart_type == 'pie':
                if len(df.columns) < 2:
                    print("Pie chart requires at least two columns of data")
                    return
                df.plot(kind='pie', y=df.columns[1], labels=df[df.columns[0]])
            
            plt.title(f"{chart_type.capitalize()} Chart of Query Result")
            plt.tight_layout()
            plt.show()
        except Exception as e:
            print(f"Error visualizing data: {e}")

    def generate_schema(self):
        schema = {}
        if self.current_db_type == "sql":
            try:
                tables = self.get_tables()
                for table in tables:
                    self.sql_cursor.execute(f"DESCRIBE {table}")
                    columns = self.sql_cursor.fetchall()
                    schema[table] = {col['Field']: col['Type'] for col in columns}
            except mysql.connector.Error as err:
                print(f"Error generating schema: {err}")
        elif self.current_db_type == "nosql":
            try:
                collections = self.get_tables()
                for collection in collections:
                    sample = self.nosql_db[collection].find_one()
                    if sample:
                        schema[collection] = {k: type(v).__name__ for k, v in sample.items()}
            except Exception as e:
                print(f"Error generating schema: {e}")
        return schema

    def suggest_queries(self, table_name):
        schema = self.generate_schema()
        if self.current_db_type == "sql":
            suggestions = [
                f"Show me all data from {table_name}",
                f"Count the total number of records in {table_name}",
                f"What is the average of unit_price in {table_name}",
                f"Show me transactions where unit_price is greater than 10",
                f"Sort the data in {table_name} by unit_price descending"
            ]
        else:
            suggestions = [
                f"show all data",
                f"count all records",
                f"find transactions where unit_price is greater than 10",
                f"sort by unit_price descending",
                f"show average unit_price"
            ]
        return suggestions

def print_table(data):
    if not data:
        print("No data to display")
        return
    try:
        # Convert ObjectId to string for MongoDB results
        if isinstance(data, list) and len(data) > 0 and '_id' in data[0]:
            for item in data:
                item['_id'] = str(item['_id'])
        print(tabulate(data, headers="keys", tablefmt="grid"))
    except Exception as e:
        print(f"Error displaying table: {e}")

def main():
    chatdb = ChatDB()
    
    while True:
        print("\n--- ChatDB Menu ---")
        print("1. Connect to SQL Database")
        print("2. Connect to NoSQL Database")
        print("3. List tables")
        print("4. View sample data")
        print("5. Execute natural language query")
        print("6. Execute custom query")
        print("7. Visualize query result")
        print("8. Generate database schema")
        print("9. Get query suggestions")
        print("10. Exit")
        
        choice = input("\nEnter your choice (1-10): ")
        
        if choice == "1":
            host = input("Enter MySQL host: ")
            user = input("Enter MySQL user: ")
            password = input("Enter MySQL password: ")
            database = input("Enter MySQL database name: ")
            if chatdb.connect_sql(host, user, password, database):
                print(f"Successfully connected to MySQL database: {database}")
            else:
                print("Failed to connect to MySQL database.")
        
        elif choice == "2":
            connection_string = input("Enter MongoDB connection string: ")
            database = input("Enter MongoDB database name: ")
            if chatdb.connect_nosql(connection_string, database):
                print(f"Successfully connected to MongoDB database: {database}")
            else:
                print("Failed to connect to MongoDB database.")
        
        elif choice == "3":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            tables = chatdb.get_tables()
            print(f"\nAvailable tables in {chatdb.current_db}:")
            for table in tables:
                print(f"- {table}")
        
        elif choice == "4":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            table_name = input("Enter table name: ")
            sample_data = chatdb.get_sample_data(table_name)
            if sample_data:
                print(f"\nSample data from {chatdb.current_db}.{table_name}:")
                print_table(sample_data)
        
        elif choice == "5":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            table_name = input("Enter table name: ")
            query = input("Enter your natural language query: ")
            result = chatdb.process_natural_language_query(table_name, query)
            if result:
                print("\nQuery result:")
                print_table(result)
        
        elif choice == "6":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            print("\nEnter your custom query:")
            if chatdb.current_db_type == "sql":
                print("Example SQL query: SELECT * FROM table_name WHERE condition")
            else:
                print("Example MongoDB query: [{'$match': {'field': 'value'}}]")
            query = input("\nEnter query: ")
            result = chatdb.execute_custom_query(query)
            if result:
                print("\nQuery result:")
                print_table(result)
        
        elif choice == "7":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            table_name = input("Enter table name: ")
            query = input("Enter your query: ")
            result = chatdb.process_natural_language_query(table_name, query)
            if result:
                print("\nQuery result:")
                print_table(result)
                chart_type = input("Enter chart type (bar/line/scatter/pie): ").lower()
                if chart_type in ['bar', 'line', 'scatter', 'pie']:
                    chatdb.visualize_data(result, chart_type)
                else:
                    print("Invalid chart type. Please choose from: bar, line, scatter, pie")
        
        elif choice == "8":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            schema = chatdb.generate_schema()
            print("\nDatabase Schema:")
            for table, columns in schema.items():
                print(f"\n{table}:")
                for column, data_type in columns.items():
                    print(f"  {column}: {data_type}")
        
        elif choice == "9":
            if not chatdb.current_db:
                print("Please connect to a database first.")
                continue
            table_name = input("Enter table name: ")
            suggestions = chatdb.suggest_queries(table_name)
            print("\nQuery Suggestions:")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"{i}. {suggestion}")
        
        elif choice == "10":
            if chatdb.sql_db:
                chatdb.sql_db.close()
            if chatdb.nosql_client:
                chatdb.nosql_client.close()
            print("\nThank you for using ChatDB. Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
