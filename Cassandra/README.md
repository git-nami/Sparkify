# Purpose:

The startup "Sparkify" is a music streaming app that collects songs and user activity in the form of csv files on its streaming platform. The project does data modelling and builds an ETL pipeline to help the analytics team to easily query the table, understand the data and get meaningful insights from the data such as which songs users are listening to.

# Source Datasets:

Below are the source datasets:
1. event_data - csv files partitioned by date containing user and song metadata.

# Target Database Design:

Currently, there is no easy way to query the data to generate the results, since the data resides in a directory of CSV files on user activity on the app. In addition, the analytics team have provided the top/most frequently used queries based on which the database needs to be modelled. Hence a Cassandra DB would be suitable for the use case which will allow the users to effectively query the DB and would support write heavy workloads.

Below are the target DB details:
1. Target DB Type: Cassandra.
2. Target Tables : music_by_session_id,music_by_user_id,music_by_song


# ETL Pipeline Design:

The ETL pipeline was implemented as follows:
1. Create a single intermediate file by extracting the required subset of columns from the individual input event files.
2. Create individual cassandra tables based on the query use case and insert data from the intermediate files.

# Implementation - Project Files:

Below are the various project files:
1. Project_1B_ Project_Template.ipynb: Python notebook containing the complete implementation.

# Execution Framework: 

Run the below notebook to create the data model and setup the ETL pipeline:
1. Project_1B_ Project_Template.ipynb
