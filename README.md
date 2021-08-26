# Project 1: Data Modeling with Postgres
## Project Description
This project is aimed to help a startup called *Sparkify* analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The challenge is that the analytics team does not have an easy way to query their data, which originally resides in JSON format for logs on user activity and metadata on the songs in the app.

Thus, the goal is to create a Postgres database `sparkify` and build an ETL pipeline using Python and SQL to optimize queries on song play analysis.

## Postgres Database Schema and ETL pipeline
The star schema includes one fact table - *songplays* and four dimention tables - *users*, *songs*, *artists*, *time*.<br>
All SQL queries to `CREATE`, `DROP`, `INSERT` into tables are contained in the **sql_queries.py** and used by the following files in ETL pipeline:
1. The file **create_tables.py** uses function `create_database` to create and connect to the sparkifydb, as well as functions `drop_tables` and `create_tables`. It should be run every time before the ETL scripts.<br>
2. The file **etl.py** reads and processes the JSON song and log datasets. The processed data from derived song files, `data/song_data`, populates *songs* and *artists* database tables, whereas the data extracted from log files, `data/log_data` is loaded into *users* and *time* tables.<br>
The fact table *songplays* uses information from the *songs* table, *artists* table, and original log files.
3. The file **test.ipynb** is used to confirm that the records have been successfully inserted into each table.
4. The file **sql_queries4analysis.ipynb** includes example SQL queries for music app data analysis.

![](sparkifydb_erd.jpg?raw=true)

## How to run the scripts
1. run the Python script **create_tables.py** in a command-line by typing `python` or `python3` depending on your Python installation
        
        python3 create_tables.py
        
and then press `Enter`. If you get no error messages, the *sparkify* database is created and the connection to the sparkifydb is set up.
2. run the Python script **etl.py** also in a command-line you used in the #1 step

        python3 etl.py
        
Then press `Enter`. This script will process the entire datasets (the number of processed files from `data/song_data` and `data/log_data` will be displayed in terminal window) and load the data into five tables (see ER diagram above).

3. a) Jypiter Notebooks **test.ipynb** and **sql_queries4analysis.ipynb** can be run from terminal with these commands:

        jupyter nbconvert --execute --clear-output test.ipynb
        
        jupyter nbconvert --execute --clear-output sql_queries4analysis.ipynb
Dont' forget to press `Enter` after every command. As next, you can open .ipynb files in IDE like *PyCharm* to see the output of cells.

   b) You can open **test.ipynb** and **sql_queries4analysis.ipynb** in PyCharm initially and execute cells code in IDE directly.

## Docstrings used in the pipeline
In the module **create_tables.py**:
        def create_database():
         """
         Description:
         This function creates the database with UTF8 encoding and connects to the sparkifydb
         Returns the connection and cursor to sparkifydb

         Arguments:
         None

         Returns: 
         cur, conn
         """

        def drop_tables(cur, conn):
         """
         Drops each table using the queries in `drop_table_queries` list.
        
         Arguments:
              cur: the cursor object. 
              conn: connection to sparkify database. 
         Returns:
         None
         """

        def create_tables(cur, conn):
         """
         Creates each table using the queries in `create_table_queries` list.
         Arguments:
              cur: the cursor object. 
              conn: connection to sparkify database. 
         Returns:
         None
         """

In the module **rtl.py**:

        def process_song_file(cur, filepath):
         """
         Description: This function can be used to read the file in the filepath (`data/song_data`)
         to get the song and artist info and used to populate the *songs* and *artists* dim tables.

         Arguments:
         cur: the cursor object. 
         filepath: song data file path. 

         Returns:
         None
         """

        def process_log_file(cur, filepath):
          """
          Description: This function can be used to read the file in the filepath (`data/log_data`)
          to get the user and time info and used to populate the *users* and *time* dim tables.

          Arguments:
              cur: the cursor object. 
              filepath: log data file path. 

          Returns:
              None
          """

        def process_data(cur, conn, filepath, func):
         """
         Description: This funcition gets all files matching extension `.json` in the filepath 
         and displays in the terminal window the number of files found and files processed
         Arguments:
              cur: the cursor object.
              conn: connection to sparkify database.
              filepath: log and song data file path.
              func: call functions `process_song_file`, `process_log_file`
         """