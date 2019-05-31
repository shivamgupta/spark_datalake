# Data Lake with Spark

__1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.__

Sparkify's rapid user growth calls for better analytical organisation and infrastructure. The purpose of this database is to organize user data into an industry standard __Star Schema__ with one fact table and some dimension tables on the AWS cloud platform.

__2. State and justify your database schema design and ETL pipeline.__

Data Engineers at Sparkify believe that the __Star Schema__ is the best way to organize user data.

__`songplays`__ is our fact table with the following columns with `songplay_id` & `user_id` as primary key:
- songplay_id (Auto generated)
- start_time 
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent


__`users`__ is one of the dimension table with the following columns:
- user_id
- first_name 
- last_name 
- gender
- level


__`songs`__ is one of the dimension table with the following columns:
- song_id 
- title
- artist_id 
- year
- duration


__`artists`__ is one of the dimension table with the following columns:
- artist_id 
- name 
- location
- latitude
- longitude


__`time`__ is one of the dimension table with the following columns:
- start_time (Cannot be NULL)
- hour (Cannot be NULL)
- day (Cannot be NULL)
- week (Cannot be NULL)
- month (Cannot be NULL)
- year (Cannot be NULL)
- weekday (Cannot be NULL)

# Command to Run
`python3 etl.py`

# Description of files

__1. `data` directory__
Contains sample data for quicker processing.
__2. `dl.cfg` __
Contains AWS IAM User credentials.
__3. `etl.py` __
ETL Pipeline that reads data from S3 and loads dimension tables back to S3 in parquet format.
