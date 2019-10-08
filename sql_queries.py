import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP STAGING TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# DROP FACT AND DIMENSION TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create = "CREATE TABLE IF NOT EXISTS \
staging_events(artist varchar, auth varchar, firstname varchar, gender char(1), iteminsession int, \
lastname varchar, length float, level varchar, location varchar, method varchar, page varchar, \
registration float, sessionid int, song text, status int, ts float, useragent text, userid int)" 

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS \
staging_songs(num_songs int, artist_id varchar, artist_latitude float, artist_longtitude float, \
artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float, year int)" 


# CREATE FACT AND DIMENSION TABLES

songplay_table_create = "CREATE TABLE IF NOT EXISTS \
songplays(songplay_id bigint identity(0,1) PRIMARY KEY, start_time timestamp , \
user_id int , level varchar, song_id varchar, artist_id varchar, \
session_id int, location varchar, user_agent varchar)"


user_table_create = "CREATE TABLE IF NOT EXISTS \
users (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, \
gender char(1), level varchar NOT NULL, UNIQUE(user_id))"


song_table_create = "CREATE TABLE IF NOT EXISTS \
songs (song_id varchar PRIMARY KEY, title text NOT NULL, artist_id varchar NOT NULL, \
year int NOT NULL, duration float NOT NULL)"


artist_table_create = "CREATE TABLE IF NOT EXISTS \
artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar NOT NULL, \
latitude float, longitude float)"

time_table_create = "CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int \
NOT NULL, day int NOT NULL, week int NOT NULL, month int NOT NULL, year int \
NOT NULL, weekday int NOT NULL)"


# STAGING TABLES ( COPY DATA FROM S3 TO STAGING TABLES)

staging_events_copy = "COPY staging_events \
from {} \
iam_role {} \
json {}".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = "COPY staging_songs \
from {} \
iam_role {} \
json 'auto' ".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])



# FINAL FACT AND DIMENSION  TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
    se.userid AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionid AS session_id,
    se.location AS location,
    se.useragent AS user_agent
    FROM staging_events se  
    LEFT JOIN staging_songs ss
        ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
        WHERE page = 'NextSong'
);
""")
    
user_table_insert = ("""
INSERT INTO users
(
    SELECT DISTINCT userid as user_id, firstname as first_name, lastname as last_name, gender, level
    FROM staging_events
    WHERE (staging_events.userid IS NOT NULL AND staging_events.level IS NOT NULL)
);
""") 

song_table_insert = ("""
INSERT INTO songs
(
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE (staging_songs.song_id IS NOT NULL AND staging_songs.title IS NOT NULL AND staging_songs.artist_id IS NOT NULL AND
          staging_songs.year IS NOT NULL AND staging_songs.duration IS NOT NULL)
);
""")

artist_table_insert = ("""
INSERT INTO artists
(
    SELECT artist_id, artist_name AS name, artist_location AS location,
    artist_latitude AS latitude, artist_longtitude AS longtitude
    FROM staging_songs
    WHERE (staging_songs.artist_name IS NOT NULL AND staging_songs.artist_location IS NOT NULL)
);
""")

time_table_insert = ("""
INSERT INTO time
(
  SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
  EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS hour,
  EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS day,
  EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS week,
  EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS month,
  EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS year,
  EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS weekday  
  FROM staging_events
  WHERE staging_events.ts IS NOT NULL  
);
""")


# QUERY LISTS

# Group queries that create all staging, fact, and dimesion tables 
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# Group queries that drop all staging, fact and dimension tables
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# Group queries that copy s3 data into staging tables 
copy_table_queries = [staging_events_copy, staging_songs_copy]

# Group queries that insert data for fact and dimension tables
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
