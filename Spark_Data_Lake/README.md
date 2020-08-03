# Purpose:
-----------
##### The startup "Sparkify" is a music streaming app that collects songs and user activity data in the form of JSON files on its streaming platform. The company has grown their user and song base further and wants to move their data warehouse solution to a data lake on cloud. The data lake will in turn be used by the analytics team who can continue to gain insights such as the songs users are listening to.

# Source Datasets:
------------------
##### The source data is stored in the form of JSON files on Amazon S3:
1. Song Dataset - JSON file containing song metadata.
2. Log Dataset - JSON file containing user activity data.

# Data Lake Design:
-------------------------
##### The project utilizes Amazon S3 to build the Data Lake and store the final target tables in parquet format. Following are the advantages of the approach:
1. Data Lake will not have the limitation of data warehouse and can store both stuctured and unstructed data, as well as data in raw format which can be modelled later.
2. An on premise solution to build the data lake would have been difficult to manage and expensive which can be avoided with cloud especially since Amazon S3 is a safe, cheap and easy to use. 
3. As the data grows , Sparkify can easily setup version control and lifecycle management policies w.r.t deletion and archival with S3.
4. The final target tables are stored on S3 in parquet format with DIMENSION modelling and utilizes a STAR schema. 
5. End users can use easily leverage spark processing framework to read the parquet files and create temporary table like structures which can be queried, joined to extract meaningful insights.
6. The final target tables are stored in parquet format which is binary and compressed , hence would occupy less space. Since its a columnar storage, users can easily perform column wise queries which will be faster.

##### Below are the target table details:
1. Fact Table : songplays
2. Dimension Tables: users, songs, artists,time.
3. File format: parquet

# ETL Pipeline Design:
----------------------
##### The ETL pipeline leverages Spark's in memory processing capabilities which is very fast and loads the data as follows:
1. Create a SparkSession and read the song_data and log_data json input files from S3 to create spark dataframes.
2. Select the desired columns for each of the target tables and perform the required transformations.
3. Save the transformed dataframe onto S3 target bucket with the required partitioning in parquet format.

# Implementation - Project Files:
----------------------------------
##### Below are the various project files:
1. etl.py: Python Script which loads the data into final parquet tables from the input S3 files.
2. dl.cfg: Configuration file which contains the AWS access key details.

# Execution Framework: 
-----------------------
##### Execute the files in the terminal in the order below:
1. python3 etl.py
2. python3 test.py (Test file, can be ignored)

