# Purpose:

The startup "Sparkify" is a music streaming app that collects songs and user activity in the form of csv files on its streaming platform. The company, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines.The project will achieve this with Apache Airflow and will build data model ,implement ETL data pipeline and include data quality checks.


# Source Datasets:

The source data is stored in the form of JSON files on Amazon S3:
1. Song Dataset - JSON file containing song metadata.
2. Log Dataset - JSON file containing user activity data.

# Data Warehouse Design:

The project utilizes Amazon AWS solution to build the data warehouse using the Redshift service in conjunction with S3. Following are the advantages of the approach:
1. An on premise solution would have been difficult to manage and expensive which can be avoided with cloud.
2. Since the source JSON data files are stored in chucks on amazon S3, we can take advantage of data locality and Redshift's massive parallel processing capabilities to load the data in parallel which can be huge time saver as the data grows.
3. The final target tables are stored with DIMENSION modelling and utilizes a STAR schema. Redshift provides a a SQL like interface on amazon console which can be easily used by the end users to join various tables and extract meaningful insights such as songs which are recently played by a particular user or songs which are most played by a user.
4. The final target tables are designed to have suitable distribution style and suitable sortkeys to speed up the join, order by queries.

Below are the target DB details:
Fact Table : songplays
Dimension Tables: users, songs, artists,time.
Intermediate Staging tables: staging_events, staging_songs.


# Airflow ETL Data Pipeline Design:

The entire ETL pipeline is automated using Airflow with re-usable tasks , monitoring and easy backfills for historic loads. Following is the implementation detail:
1. Drop the tables if they already exists.
2. Create an intermediate staging(staging_events, staging_songs) by copying the raw data from the source S3 JSON files present in the log_data and song_data folder respectively into Redshift.
3. Create songs and artists dimension table on Redshift by extracting a subset of columns and loading the data directly from staging_songs table.
4. Create users and time dimension table on Redshift by extracting a subset of columns and loading the data directly from staging_events table.
5. Create songplays fact table by joining the 2 staging tables and extracting a subset of columns from these tables directly.

[Sparkify DAG](https://github.com/git-nami/Sparkify/blob/master/Airflow/Airlow_pipeline.png)


# Execution Framework: 

The below DAG needs to be scheduled by passing suitable values to start_date default param.
1. udac_example_dag.py

