# Purpose:
-----------
### The startup "Sparkify" is a music streaming app that collects songs and user activity in the form of JSON files on its streaming platform. The project does data modelling and builds an ETL pipeline to help the analytics team to easily query the table, understand the data and get meaningful insights from the data such as which songs users are listening to.

# Source Datasets:
------------------
### Below are the source datasets:
1. Song Dataset - JSON file containing song metadata.
2. Log Dataset - JSON file containing user activity data.

# Target Database Design:
-------------------------
### The data "SPARKIFY" receives is in the form of JSON files, which is not easy to read , understand or use. In order for the analytics team to effectively query the data, the data needs to be imported into a more simplified Postgres database with a STAR schema. This will allow the users to easily join various tables and extract meaningful insights such as songs which are recently played by a particular user or songs which are most played by a user etc.

### Below are the target DB details:
1. Target DB Type: Postgres
2. Model Choice: Star Schema(1 fact table and 4 dimension tables)
3. Fact Table : songplays
4. Dimension Tables: users, songs, artists,time.

### Below is the ER diagram:
![ER_diagram](Song_ERD.png)

# ETL Pipeline Design:
----------------------
### The ETL pipeline was implemented as follows:
1. Create songs and artists dimension table by extracting a subset of columns from the files in the songs_data folder.
2. Create users and time dimension table by extracting a subset of columns from the files in the log_data folder.
3. Create songplays fact table by using the primary keys from the 4 dimension tables and a subset of columns from the files in the log_data folder.

# Implementation - Project Files:
----------------------------------
### Below are the various project files:
1. sql_queries.py: Configuration file containing create, insert and drop DML statements for individual tables.
2. create_tables.py: Python script which calls sql_queries.py to drop and create individual tables.
3. etl.ipynb: Python notebook to prototype and test the ETL pipeline to load the individual tables.
4. etl.py: Actual Python Script which mimics the logic in etl.ipynb to build the ETL pipeline.
5. test.ipynb: Python notebook containing select statements to test the execution of following modules: create_tables.py, etl.ipynb, etl.py.

# Execution Framework: 
-----------------------
### Execute the files in the terminal in the order below to create the data model and setup the ETL pipeline:
1. python3 create_tables.py
2. python3 etl.py
3. test.ipynb

