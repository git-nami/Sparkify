import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import types as t


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    #spark = SparkSession \
    #    .builder \
    #    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    #    .getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    return spark


def test_data(spark):
    """songs_path = "data/songs"
    df_songs = spark.read.parquet(songs_path)
    df_songs.printSchema()
    df_songs.createOrReplaceTempView("songs_table_temp")
    spark.sql("select * from songs_table_temp limit 5").show(5,False)
    
    artists_path = "data/artists"
    df_artists = spark.read.parquet(artists_path)
    df_artists.printSchema()
    df_artists.createOrReplaceTempView("artists_table_temp") 
    spark.sql("select * from artists_table_temp limit 5").show(5,False)

    
    users_path = "data/users"
    df_users = spark.read.parquet(users_path)
    df_users.printSchema()
    df_users.createOrReplaceTempView("users_table_temp")
    spark.sql("select * from users_table_temp limit 5").show(5,False)
    
    time_path = "data/time"
    df_time = spark.read.parquet(time_path)
    df_time.printSchema()
    df_time.createOrReplaceTempView("time_table_temp")
    spark.sql("select * from time_table_temp limit 5").show(5,False)"""
    
    songplays_path = "data/songplays"
    df_songplays = spark.read.parquet(songplays_path)
    df_songplays.printSchema()
    df_songplays.createOrReplaceTempView("songplays_table_temp")
    #spark.sql("select * from songplays_table_temp limit 5").show(5,False)
    spark.sql("select count(*) from songplays_table_temp").show()
    
def main():
    spark = create_spark_session()
    
    test_data(spark) 
    spark.stop()
    


if __name__ == "__main__":
    main()
