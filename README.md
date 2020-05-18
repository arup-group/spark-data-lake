# Analytics data lake

This project builds a data pipeline for ETL of song and songplay data for building a data lake. The data pipeline extracts data from Amazon S3, processes it using Spark, and loads the data back into Amazon S3 as a set of parquet files. This will allow an analytics team to continue finding insights in what songs their users are listening to.

## Database Schema Design
The data lake is designed to follow a star schema, optimized for queries on song play analysis for analytics. It includes the following tables and attributes:

Fact Table:
    
    songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables:
    
    users - user_id, first_name, last_name, gender, level
    songs - song_id, title, artist_id, year, duration
    artists - artist_id, name, location, lattitude, longitude
    time - start_time, hour, day, week, month, year, weekday


## ETL Pipeline

The data is stored inside Amazon S3 buckets in JSON files. There are two different datasets - log_data and song_data. The data is extracted and processesed using pySpark, and loaded back into S3 as a set of dimensional tables.

## How to run

To run the ETL pipeline simply execute the script etl.py on your terminal:

```
python etl.py
```
