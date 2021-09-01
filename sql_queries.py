# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
log_files_staging_table_drop = "DROP TABLE IF EXISTS log_files_staging"

# CREATE TABLES


songplay_table_create = ("""
CREATE SEQUENCE songplay_id_seq;
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int NOT NULL DEFAULT nextval('songplay_id_seq'), 
start_time timestamp, 
userId int NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id int NOT NULL, 
location varchar, 
user_agent varchar, 
PRIMARY KEY (songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
userId int NOT NULL, 
firstName varchar(15), 
lastName varchar(20), 
gender varchar(6), 
level varchar, 
PRIMARY KEY (userId)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar NOT NULL, 
title varchar, 
artist_id varchar NOT NULL, 
year int, 
duration float, 
PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar, 
artist_name varchar, 
artist_location varchar, 
artist_latitude float, 
artist_longitude float, 
PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar(10), 
PRIMARY KEY (start_time)
);
""")

log_files_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS log_files_staging (
start_time timestamp, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar(10),
userId int NOT NULL, 
firstName varchar(15), 
lastName varchar(20), 
gender varchar(6), 
level varchar,
artist varchar,
song varchar,
length float,
session_id int NOT NULL, 
location varchar, 
user_agent varchar
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, user_agent)\
VALUES(DEFAULT,%s::timestamp,%s,%s,%s,%s,%s,%s,%s);
""")

songplay_table_staging_insert = ("""
INSERT INTO songplays 
SELECT nextval('songplay_id_seq'), lfs.start_time, lfs.userId, lfs.level, sa.song_id, sa.artist_id, lfs.session_id, lfs.location, lfs.user_agent
FROM (SELECT s.song_id, a.artist_id, s.title, a.artist_name, s.duration
FROM songs s 
JOIN artists a ON s.artist_id=a.artist_id) sa
RIGHT JOIN log_files_staging lfs ON sa.title=lfs.song and sa.artist_name=lfs.artist and sa.duration=lfs.length
""")

user_table_insert = ("""
INSERT INTO users (userId, firstName, lastName, gender, level) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (userId) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES(%s::timestamp,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING;
""")

time_table_staging_insert = ("""
INSERT INTO time
SELECT rt.start_time, rt.hour, rt.day, rt.week, rt.month, rt.year, rt.weekday 
FROM (SELECT log.start_time, log.hour, log.day, log.week, log.month, log.year, log.weekday, ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY start_time DESC) AS rn
FROM log_files_staging log) rt
WHERE rn = 1
""")

#MERGE USER RECORDS

merge_user_table = ("""
INSERT INTO users 
SELECT lfs.userId, lfs.firstName, lfs.lastName, lfs.gender, lfs.level
       FROM (SELECT log.*, ROW_NUMBER() OVER (PARTITION BY log.userId ORDER BY log.start_time DESC) AS rn
       FROM log_files_staging log
            ) lfs
        WHERE rn = 1       
ON CONFLICT (userId) DO UPDATE SET level=EXCLUDED.level;
""")

#merge_user_table1 = ("""
#MERGE INTO users u 
#USING  (SELECT lfs.userId, lfs.firstName, lfs.lastName, lfs.gender, lfs.level
#       FROM (SELECT log.*, ROW_NUMBER() OVER (PARTITION BY log.userId ORDER BY log.start_time DESC) AS rn
#       FROM log_files_staging log
#            ) lfs
#        WHERE rn = 1) lfs1
        
#ON lfs1.userId = u.userId

#WHEN NOT MATCHED 
#    INSERT (userId, firstName, lastName, gender, level)
#    VALUES (lfs1.userId, lfs1.firstName, lfs1.lastName, lfs1.gender, lfs1.level)
    
#WHEN MATCHED 
#  UPDATE SET level = lfs1.level
#""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s 
JOIN artists a ON s.artist_id=a.artist_id
WHERE s.title = %s AND a.artist_name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,log_files_staging_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, log_files_staging_table_drop]