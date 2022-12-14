import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"


# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id BIGINT NULL,
        artist VARCHAR NULL,
        auth VARCHAR NULL,
        firstName VARCHAR NULL,
        gender VARCHAR NULL,
        itemInSession INTEGER NULL,
        lastName VARCHAR NULL,
        length DECIMAL(9) NULL,
        level VARCHAR NULL,
        location VARCHAR NULL,
        method VARCHAR NULL,
        page VARCHAR NULL,
        registration VARCHAR NULL,
        sessionId INTEGER NOT NULL SORTKEY DISTKEY,
        song VARCHAR NULL,
        status INTEGER NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR NULL,
        userId INTEGER NULL
    ) diststyle key;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER NULL,
        artist_id VARCHAR NULL,
        artist_latitude DECIMAL(9) NULL,
        artist_longitude DECIMAL(9) NULL,
        artist_location VARCHAR NULL,
        artist_name VARCHAR NULL,
        song_id VARCHAR NULL SORTKEY DISTKEY,
        title VARCHAR NULL,
        duration DECIMAL(9) NULL,
        year INTEGER NULL
    ) diststyle key;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER PRIMARY KEY SORTKEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL DISTKEY,
        artist_id VARCHAR NOT NULL,
        session_id VARCHAR NOT NULL,
        location VARCHAR NULL,
        user_agent VARCHAR NULL
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY SORTKEY,
        first_name VARCHAR NULL,
        last_name VARCHAR NULL,
        gender VARCHAR(1) NULL,
        level VARCHAR NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY SORTKEY DISTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration DECIMAL(9) NOT NULL
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR NULL,
        location VARCHAR NULL,
        latitude DECIMAL(9) NULL,
        longitude DECIMAL(9) NULL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour SMALLINT NULL,
        day SMALLINT NULL,
        week SMALLINT NULL,
        month SMALLINT  NULL,
        year SMALLINT NULL,
        weekday SMALLINT NULL
    ) diststyle all;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        DISTINCT (TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
        FROM staging_events AS se
        INNER JOIN staging_songs AS ss
            ON se.artist = ss.artist_name 
            AND se.song = ss.title
            AND se.length = ss.duration
        WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT  
        DISTINCT se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender AS gender,
        se.level AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists 
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT  
        DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(week FROM start_time) AS weekday
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
