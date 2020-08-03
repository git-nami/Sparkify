import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN= config.get("IAM_ROLE","ARN")
LOG_DATA_PATH= config.get("S3","LOG_DATA")
SONG_DATA_PATH= config.get("S3","SONG_DATA")
LOG_JSONPATH= config.get("S3","LOG_JSONPATH")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender TEXT,
    item_in_session TEXT,
    last_name TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration NUMERIC,
    session_id INT,
    song TEXT,
    status INT,
    ts TIMESTAMP,
    user_agent TEXT,
    user_id INT)
    """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
   num_songs INT, 
   artist_id TEXT,
   artist_latitude NUMERIC, 
   artist_longitude NUMERIC, 
   artist_location TEXT, 
   artist_name TEXT, 
   song_id TEXT, 
   title TEXT, 
   duration NUMERIC, 
   year INT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
   songplay_id BIGINT IDENTITY(0,1) NOT NULL, 
   start_time TIMESTAMP NOT NULL SORTKEY, 
   user_id INT NOT NULL DISTKEY, 
   level TEXT, 
   song_id TEXT NOT NULL,
   artist_id TEXT NOT NULL, 
   session_id INT, 
   location TEXT, 
   user_agent TEXT)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL SORTKEY DISTKEY, 
    first_name TEXT NOT NULL, 
    last_name TEXT NOT NULL, 
    gender TEXT, 
    level TEXT)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT NOT NULL SORTKEY, 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INT, 
    duration NUMERIC)diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT NOT NULL SORTKEY, 
    name TEXT NOT NULL, 
    location TEXT, 
    lattitude NUMERIC, 
    longitude NUMERIC)diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL SORTKEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT)diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
    credentials 'aws_iam_role={}' 
    json {} TIMEFORMAT as 'epochmillisecs' region 'us-west-2';
""").format(LOG_DATA_PATH,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {} 
    credentials 'aws_iam_role={}' 
    json 'auto' region 'us-west-2';
""").format(SONG_DATA_PATH,DWH_ROLE_ARN)

# FINAL TABLES

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
   SELECT DISTINCT user_id,
   first_name,
   last_name,
   gender,
   level
   from staging_events
   WHERE page = 'NextSong'
   and user_id is not null""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
   SELECT DISTINCT song_id,
   title,
   artist_id,
   year,
   duration
   FROM staging_songs
   WHERE song_id is not null""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, lattitude, longitude)
   SELECT DISTINCT artist_id,
   artist_name,
   artist_location,
   artist_latitude,
   artist_longitude
   FROM staging_songs
   WHERE artist_id is not null""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
   SELECT DISTINCT ts AS start_time,
    EXTRACT(hour from ts) AS hour,
    EXTRACT(day from ts) AS day,
    EXTRACT(week from ts) As week,
    EXTRACT(month from ts) AS month,
    EXTRACT(year from ts) AS year,
    EXTRACT(weekday from ts) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
    and ts is not null""")

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT se.ts AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
    AND se.page = 'NextSong'""")


# VALIDATE TABLES
select_songplays = "SELECT * from songplays LIMIT 5"
select_users = "SELECT * from users LIMIT 5"
select_artists = "SELECT * from artists LIMIT 5"
select_songs = "SELECT * from songs LIMIT 5"
select_time = "SELECT * from time LIMIT 5"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
select_table_queries = [select_songplays,select_users,select_artists,select_songs,select_time]
