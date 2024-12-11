# ChatDB: Interactive Database Query Assistant
An interactive command-line application that allows users to query both SQL and MongoDB databases using natural language input.

# Files in the Project

chatdb.py - Main application with database querying and visualization capabilities
database_setup.py - Utility to set up databases and import data
coffee_shop_sales.csv - Sample dataset for testing

# Installation

Install required dependencies:

pip install mysql-connector-python pymongo nltk pandas matplotlib tabulate numpy sqlalchemy

Download NLTK data (run in Python):

import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Usage

First, set up your database using the setup utility:

python database_setup.py

When prompted:

**1) For MySQL: Enter host, user, password, and database name**
**2) For MongoDB: Enter connection string and database name**


Run the ChatDB application:
python chatdb.py

# Features

Database Support:


MySQL (SQL)
MongoDB (NoSQL)


Natural Language Queries:

CopyMySQL Examples:
- "Show me all data"
- "Count all records"
- "Find transactions where unit_price is greater than 10"
- "Sort by unit_price descending"
- "Show average unit_price"

MongoDB Examples:
- "show all data"
- "count all records"
- "find transactions where unit_price is greater than 10"
- "sort by unit_price descending"

Data Visualization:


Bar charts
Line charts
Scatter plots
Pie charts


Additional Features:


Schema generation
Sample data viewing
Custom query execution
Query suggestions

# Database Connection Details

# MySQL:
CopyHost: localhost
Default user: chatdb_user
Default password: your_password

# MongoDB:
CopyDefault connection string: mongodb://localhost:27017/

# Common Commands and Examples

Import data:

bash
#Run setup utility
python database_setup.py

#Choose option 1 for MySQL or 2 for MongoDB
#Follow the prompts to import your CSV file

Query data:

bash
#Run main application
python chatdb.py

#Connect to database (option 1 or 2)
#Use option 5 for natural language queries
#Use option 7 for data visualization

# Error Handling and Troubleshooting

Database Connection Issues:


Verify MySQL/MongoDB is running
Check credentials
Ensure database exists


Import Issues:


Verify CSV file format
Check file permissions
Ensure sufficient disk space


Query Issues:


Check table/collection exists
Verify column names in queries
Ensure data types match

# Requirements

Python 3.7+
MySQL Server
MongoDB Server
Required Python packages (install via pip):

mysql-connector-python
pymongo
nltk
pandas
matplotlib
tabulate
numpy
sqlalchemy



# Note
Make sure to have MySQL and MongoDB servers running before using the application.
