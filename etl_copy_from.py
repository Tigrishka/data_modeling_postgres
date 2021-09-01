import os
import io
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, song_json_files):
    """
    Description: This function reads JSON files in the filepath (`data/song_data`) into DataFrame, concatenates it
    and drops duplicates on `song_id` and `artist_id`

     Arguments:
     cur: the cursor object. 
     filepath: song data file path. 

     Returns:
     song_files_concat: DataFrame of concatenated song files with removed duplicates
     """
            
    all_song_files = []
    for song_f in song_json_files:
        df = pd.read_json(song_f, lines=True)
        all_song_files.append(df)
    song_files_concat = pd.concat(all_song_files).drop_duplicates('song_id').drop_duplicates('artist_id').replace(np.nan, 0)
    return song_files_concat

def songs_table(song_files_list, cur, conn):
    """
    Description: 
    This function transforms data from song_files_list into CSV with customized fields 
    and loads it into the database dim table *songs* using a `copy_from` function.
    
    Arguments:
     cur: the cursor object.
     conn: connection to sparkify database.
     song_files_list: DataFrame of all song data generated in the function `process_song_file`. 

     Returns:
     None
    """
    
    song_files_fields = song_files_list[['song_id', 'title', 'artist_id', 'year', 'duration']]
#    print(song_files_fields)
    song_files_csv = song_files_fields.to_csv(header=False, sep='|', index=False)
#    print(song_files_csv)
    csv_file_like_object = io.StringIO()
    csv_file_like_object.write(song_files_csv)
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, 'songs', sep='|')
    conn.commit()

def artists_table(song_files_list, cur, conn):
    """
    Description: 
    This function transforms data from song_files_list into CSV with customized fields
    and loads it into the database dim table *artists* using a `copy_from` function.
    
    Arguments:
     cur: the cursor object. 
     conn: connection to sparkify database.
     song_files_list: DataFrame of all song data generated in the function `process_song_file`. 

     Returns:
     None
    """
    
    song_files_fields = song_files_list[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
#    print(song_files_fields)
    song_files_csv = song_files_fields.to_csv(header=False, sep='|', index=False)
#    print(song_files_csv)
    csv_file_like_object = io.StringIO()
    csv_file_like_object.write(song_files_csv)
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, 'artists', sep='|')
    conn.commit()

def process_log_file(cur, log_json_files):
    """
    Description: This function reads files in the filepath (`data/log_data`) into DataFrame,
    extract the timestamp (`start_time`), hour, day, week of year (`week`), month, year, and weekday from the `ts` column, 
    concatenates DataFrames
    and filter records by `NextSong` action

     Arguments:
     cur: the cursor object. 
     filepath: song data file path. 

     Returns:
     log_files_concat: DataFrame of concatenated log files filtered by records `NextSong`
     """
    
    all_log_files = []
    for log_f in log_json_files:
        df = pd.read_json(log_f, lines=True)
        t = pd.to_datetime(df['ts'], unit='ms').dt.round('S')
        df['start_time'] = t
        df['hour'] = t.dt.hour
        df['day'] = t.dt.day
        df['week'] = t.dt.week
        df['month'] = t.dt.month
        df['year'] = t.dt.year
        df['weekday'] = t.dt.weekday
        all_log_files.append(df)
    log_files_concat = pd.concat(all_log_files)
    log_files_concat = log_files_concat.loc[log_files_concat['page']=='NextSong']  
    return log_files_concat

def log_files_staging_table(log_files_list, cur, conn):
    """
    Description: 
    This function transforms data from log_files_list into CSV with customized fields
    and loads it into the database staging table *log_files_staging* using a `copy_from` function.
    
    Arguments:
     cur: the cursor object. 
     conn: connection to sparkify database.
     log_files_list: DataFrame of all song data generated in the function `process_song_file`. 

     Returns:
     None
    """
    
    log_files_fields = log_files_list[['start_time','hour','day','week','month','year','weekday',
                                       'userId','firstName','lastName','gender','level','artist',
                                       'song','length','sessionId','location','userAgent']]
    log_files_csv = log_files_fields.to_csv(header=False, sep='|', index=False)
    csv_file_like_object = io.StringIO()
    csv_file_like_object.write(log_files_csv)
    csv_file_like_object.seek(0)
    cur.copy_from(csv_file_like_object, 'log_files_staging', sep='|')
    conn.commit()

def process_data(cur, conn, filepath):
    """
    Description: This function gets all files matching extension `.json` in the filepath 
     
     Arguments:
          cur: the cursor object.
          conn: connection to sparkify database.
          filepath: log and song data file path.
    
    Returns:
        all_files: the list of all JSON files in the filepath
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files   


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data')  
    song_json_files = process_data(cur, conn, filepath='data/song_data')
    
    process_data(cur, conn, filepath='data/log_data')
    log_json_files = process_data(cur, conn, filepath='data/log_data')
    
    process_song_file(cur, song_json_files)
    song_files_list = process_song_file(cur, song_json_files)

    songs_table(song_files_list, cur, conn)
    artists_table(song_files_list, cur, conn)
    
    process_log_file(cur, log_json_files)
    log_files_list = process_log_file(cur, log_json_files)

    log_files_staging_table(log_files_list, cur, conn)

    cur.execute(time_table_staging_insert)
    conn.commit()
    cur.execute(merge_user_table)
    conn.commit()
    cur.execute(songplay_table_staging_insert)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()