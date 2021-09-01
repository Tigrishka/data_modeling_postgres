# Project 1: Data Modeling with Postgres
## Project Description
This project is aimed to help a startup called *Sparkify* analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The challenge is that the analytics team does not have an easy way to query their data, which originally resides in JSON format for logs on user activity and metadata on the songs in the app.

Thus, the goal is to create a Postgres database `sparkify` and build an ETL pipeline using Python and SQL to optimize queries on song play analysis.

## Postgres Database Schema and ETL pipeline
The star schema includes one fact table - *songplays* and four dimention tables - *users*, *songs*, *artists*, *time*.<br>
All SQL queries to `CREATE`, `DROP`, `INSERT` into tables are contained in the **sql_queries.py** and used by the following files in ETL pipeline:
1. The file **create_tables.py** uses function `create_database` to create and connect to the sparkifydb, as well as functions `drop_tables` and `create_tables`. It should be run every time before the ETL scripts.<br>
2.  a) The file **etl.py** reads and processes the JSON song and log datasets using the INSERT command.<br> 
    b) The file **etl_copy_from.py** reads and processes the JSON song and log datasets using the COPY command. This approach is the best way to load data into a database.<br>
The processed data from derived song files, `data/song_data`, populates *songs* and *artists* database tables, whereas the data extracted from log files, `data/log_data` is loaded into *users* and *time* tables.<br>
The fact table *songplays* uses information from the *songs* table, *artists* table, and original log files.
3. The file **test.ipynb** is used to confirm that the records have been successfully inserted into each table.
4. The file **sql_queries4analysis.ipynb** includes example SQL queries for music app data analysis.

![](sparkifydb_erd.jpg?raw=true)

The *log_files_staging* table stores the log data and is used to load the data into *time*, *users*, and *songplays* tables using the ETL pipeline with the COPY command.

![](log_files_staging.jpg?raw=true)

## How to run the scripts
1. run the Python script **create_tables.py** in a command-line by typing `python` or `python3` depending on your Python installation
        
        python3 create_tables.py
        
and then press `Enter`. If you get no error messages, the *sparkify* database is created and the connection to the sparkifydb is set up.

2. User has two options for ETL pipelines<br>
a) to run the ETL pipeline with INSERT statement which inserts rows one by one. This way is not very efficient and is very slow.
    Run the Python script **etl.py** also in a command-line you used in the #1 step

        python3 etl.py
    
b) The best alternative to load data into a database is using the COPY command.
    Run the Python script **etl_copy_from.py** also in a command-line you used in the #1 step

        python3 etl_copy_from.py

In both options a) and b), press `Enter` after a command. Both scripts will process the entire datasets and load the data into five tables (see ER diagram above).

3. a) Jypiter Notebooks **test.ipynb** and **sql_queries4analysis.ipynb** can be run from terminal with these commands:

        jupyter nbconvert --execute --clear-output test.ipynb
        
        jupyter nbconvert --execute --clear-output sql_queries4analysis.ipynb
Dont' forget to press `Enter` after every command. As next, you can open .ipynb files in IDE like *PyCharm* to see the output of cells.

   b) You can open **test.ipynb** and **sql_queries4analysis.ipynb** in PyCharm initially and execute cells code in IDE directly.
