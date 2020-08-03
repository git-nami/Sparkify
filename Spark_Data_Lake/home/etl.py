import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initiates a spark session to be used by subsequent functions.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    #spark = SparkSession.builder.getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads the json files from the song_data folder in S3 , selects a subset of columns and creates
    the target table parquet files for songs and artists with suitable partitioning.
    Arguments:
    spark: SparkSession object.
    input_data: S3 bucket for input files.
    output_data: S3 bucket for output files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    #create a temp table from the song_data file for later use in the process_log_data module
    df.createOrReplaceTempView("song_table_temp") 
    
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
    

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as lattitude", "artist_longitude as longitude"]).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")



def process_log_data(spark, input_data, output_data):
    """
    Reads the json files from the log_data folder in S3 , selects a subset of columns and creates
    the target table parquet files for users, time and songplays with suitable partitioning.
    Arguments:
    spark: SparkSession object.
    input_data: S3 bucket for input files.
    output_data: S3 bucket for output files.
    """

    # get filepath to log data file
    log_data = input_data + "log_data/"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')


    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    df = df.withColumn("start_time",df.ts.cast(t.DoubleType()))
    df = df.withColumn("start_time",to_timestamp(df.ts/1000))

    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = df.withColumn("start_time",F.to_timestamp(df["start_time"]))
 
    # extract columns to create time table
    df = df.withColumn("hour",hour("start_time"))
    df = df.withColumn("day",dayofmonth("start_time"))
    df = df.withColumn("week",weekofyear("start_time"))
    df = df.withColumn("year",year("start_time"))
    df = df.withColumn("month",month("start_time"))
    df = df.withColumn("weekday",dayofweek("start_time"))
    
    time_table = df.select("start_time","hour","day","week","month","year","weekday").distinct()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")


    # read in song data to use for songplays table
    song_df = spark.sql("select song_id, artist_id, title, artist_name, duration FROM song_table_temp")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title ) & (df.artist == song_df.artist_name) & (df.length == song_df.duration)) \
    .distinct() \
    .withColumn("songplay_id",monotonically_increasing_id()) \
    .selectExpr("songplay_id","start_time","userId as user_id","level","song_id","artist_id","sessionId as session_id","location","userAgent as user_agent","year","month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://namita-jetsons-udacity-dend/"
    
    #Input paths for files in the local workspace to test with small chuck of data
    #input_data = "data/"
    #output_data = "data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
