# Purpose:
-----------
##### The startup "Sparkify" is a music streaming app that collects songs and user activity data in the form of JSON files on its streaming platform. The company has grown their user and song base further and wants to move to a  data warehouse solution on cloud. The data warehouse will in turn be used by the analytics team who can continue to gain insights such as the songs users are listening to.

# Source Datasets:
------------------
##### The source data is stored in the form of JSON files on Amazon S3:
1. Song Dataset - JSON file containing song metadata.
2. Log Dataset - JSON file containing user activity data.

# Data Warehouse Design:
-------------------------
##### The project utilizes Amazon AWS solution to build the data warehouse using the Redshift service in conjunction with S3. Following are the advantages of the approach:
1. An on premise solution would have been difficult to manage and expensive which can be avoided with cloud. 
2. Since the source JSON data files are stored in chucks on amazon S3, we can take advantage of data locality and Redshift's massive parallel processing capabilities to load the data in parallel which can be huge time saver as the data grows.
3. The final target tables are stored with DIMENSION modelling and utilizes a STAR schema. Redshift provides a a SQL like interface on amazon console which can be easily used by the end users to join various tables and extract meaningful insights such as songs which are recently played by a particular user or songs which are most played by a user.
4. The final target tables are designed to have suitable distribution style and suitable sortkeys to speed up the join, order by queries.

##### Below are the target DB details:
1. Fact Table : songplays
2. Dimension Tables: users, songs, artists,time.
3. Intermediate Staging tables: staging_events, staging_songs.


# ETL Pipeline Design:
----------------------
##### The ETL pipeline leverages Redshift's massive parallel processing capabilities and loads the data as follows:
1. Drop the tables if they already exists.
1. Create an intermediate staging(staging_events, staging_songs) by copying the raw data from the source S3 JSON files present in the log_data and song_data folder respectively into Redshift.
2. Create songs and artists dimension table on Redshift by extracting a subset of columns and loading the data directly from staging_songs table.
2. Create users and time dimension table on Redshift by extracting a subset of columns  and loading the data directly from staging_events table.
3. Create songplays fact table by joining the 2 staging tables and extracting a subset of columns from these tables directly.

# Implementation - Project Files:
----------------------------------
##### Below are the various project files:
1. sql_queries.py: Configuration file containing create, copy, insert and drop DML statements for individual tables.
2. create_tables.py: Python script which calls sql_queries.py to drop and create individual tables.
3. etl.py: Python Script which calles sql_queries.py and  loads the data into staging and final target tables.
4. dwh.cfg: Configuration file which contains the AWS Redshift connection and S3 file path details.

# Execution Framework: 
-----------------------
##### Execute the files in the terminal in the order below to create the data model and setup the ETL pipeline:
1. python3 create_tables.py
2. python3 etl.py
3. python3 test.py (Test file, can be ignored)

