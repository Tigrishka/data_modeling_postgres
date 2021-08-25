# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial NOT NULL, 
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
duration float, PRIMARY KEY (song_id)
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

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, user_agent)\
VALUES(DEFAULT,%s::timestamp,%s,%s,%s,%s,%s,%s,%s);
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

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s 
JOIN artists a ON s.artist_id=a.artist_id
WHERE s.title = %s AND a.artist_name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]