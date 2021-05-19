import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')



# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"



# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    event_id            INT        IDENTITY(0,1) PRIMARY KEY,
                                    artist              VARCHAR,
                                    auth                VARCHAR,
                                    firstName           VARCHAR, 
                                    gender              VARCHAR,
                                    iteminSession       VARCHAR,
                                    lastName            VARCHAR,
                                    length              DECIMAL,
                                    level               VARCHAR,
                                    location            VARCHAR,
                                    method              VARCHAR,
                                    page                VARCHAR,
                                    registration        VARCHAR,
                                    sessionId           INT,
                                    song                VARCHAR,
                                    status              INT,
                                    ts                  BIGINT,
                                    userAgent           VARCHAR,
                                    userId              INT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs        INT,
                                    artist_id        VARCHAR,
                                    artist_latitude  VARCHAR,
                                    artist_longitude VARCHAR,
                                    artist_location  VARCHAR,
                                    artist_name      VARCHAR,
                                    song_id          VARCHAR,
                                    title            VARCHAR,
                                    duration         DECIMAL,
                                    year             INT
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id      INT         IDENTITY(0,1) PRIMARY KEY, 
                                start_time       TIMESTAMP   NOT NULL, 
                                user_id          INT         NOT NULL, 
                                level            VARCHAR, 
                                song_id          VARCHAR, 
                                artist_id        VARCHAR, 
                                session_id       INT, 
                                location         VARCHAR, 
                                user_agent       VARCHAR
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id      INT        PRIMARY KEY, 
                            first_name   VARCHAR, 
                            last_name    VARCHAR, 
                            gender       VARCHAR, 
                            level        VARCHAR
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id      VARCHAR    PRIMARY KEY, 
                            title        VARCHAR, 
                            artist_id    VARCHAR, 
                            year         INT, 
                            duration     DECIMAL
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id    VARCHAR PRIMARY KEY, 
                            name         VARCHAR, 
                            location     VARCHAR, 
                            latitude     VARCHAR, 
                            longitude    VARCHAR
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time   TIMESTAMP   PRIMARY KEY, 
                            hour         INT, 
                            day          INT, 
                            week         INT, 
                            month        INT, 
                            year         INT, 
                            weekday      INT
);
""")



# STAGING TABLES
DWH_ROLE_ARN = "arn:aws:iam::278201544943:role/dwhRole"
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'

staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log_data'
                            credentials 'aws_iam_role={}'
                            format as json '{}'
                            STATUPDATE ON
                            region 'us-west-2';
""").format(DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM 's3://udacity-dend/song_data'
                            credentials 'aws_iam_role={}'
                            format as json 'auto'
                            ACCEPTINVCHARS AS '^'
                            STATUPDATE ON
                            region 'us-west-2';
""").format(DWH_ROLE_ARN)



# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays (start_time, 
                                                   user_id, 
                                                   level, 
                                                   song_id, 
                                                   artist_id, 
                                                   session_id, 
                                                   location, 
                                                   user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
       e.userId AS user_id,
       e.level AS level,
       s.song_id AS song_id,
       s.artist_id AS artist_id,
       e.sessionId AS session_id,
       e.location AS location,
       e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON (e.artist=s.artist_name)
WHERE e.page='NextSong';
""")

user_table_insert = ("""INSERT INTO users (user_id, 
                                           first_name, 
                                           last_name, 
                                           gender, 
                                           level)
SELECT DISTINCT userId AS user_id, 
       firstName AS first_name, 
       lastName AS last_name, 
       gender AS gender, 
       level AS level
FROM staging_events
WHERE page='NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, 
                                           title, 
                                           artist_id, 
                                           year, 
                                           duration)
SELECT DISTINCT song_id AS song_id,
       title AS title,
       artist_id AS artist_id,
       year AS year,
       duration AS duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, 
                                               name, 
                                               location, 
                                               latitude, 
                                               longitude)
SELECT DISTINCT artist_id AS artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS latitude,
       artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, 
                                          hour, 
                                          day, 
                                          week, 
                                          month, 
                                          year, 
                                          weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEK FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(DOW FROM start_time) AS weekday       
FROM staging_events
WHERE page='NextSong';
""")



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
