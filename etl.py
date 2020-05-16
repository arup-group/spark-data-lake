import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session or gets session in case one already exists
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data from S3 and process it and save to output location in S3

    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """


    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(path)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = df.write.partitionBy('year', 'artist').parquet(output_data + '/songs/songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(f.col('artist_id'), 
                              f.col('artist_name').alias('name'), 
                              f.col('artist_location').alias('location'), 
                              f.col('artist_latitude').alias('latitude'), 
                              f.col('artist_longitude').alias('longitude')) \
                              .dropDuplicates()
    
    # write artists table to parquet files
    artists_table = df.write.parquet(output_data + '/artists/artists.parquet')


def process_log_data(spark, input_data, output_data):
    """
    Read log data from S3, process it and save to output location in S3

    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """

    # get filepath to log data file
    log_data = input_data  + '/log_data/*.json'

    # read log data file
    df = spark.read.json(path)
    
    # filter by actions for song plays
    df = df.filter((f.col('page')=='NextSong'))

    # extract columns for users table    
    users_table = df.select(f.col('userId').alias('user_id'), 
                            f.col('firstName').alias('first_name'), 
                            f.col('lastName').alias('last_name'),
                            f.col('gender'), 
                            f.col('level')) \
                            .dropDuplicates()
    
    # write users table to parquet files
    users_table = df.write.parquet(output_data + '/users/users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x))/1000), StringType())
    df = df.withColum('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)), StringType())
    df = df.withColum('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(f.col('datetime').alias('start_time')) \
                    .withColumn('hour', f.hour('start_time')) \
                    .withColumn('day', f.day('start_time')) \
                    .withColumn('week', f.week('start_time')) \
                    .withColumn('month', f.month('start_time')) \
                    .withColumn('year', f.year('start_time')) \
                    .withColumn('weekday', f.dayofweek('start_time')) \
                    .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year', 'month').parquet(output_data + '/time/time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, f.col('df.artist') == f.col('song_df.artist_name'), 'inner') \
                        .select(f.col('df.datetime').alias('start_time'),
                                f.col('df.userId').alias('user_id'),
                                f.col('df.level').alias('level'),
                                f.col('song_df.song_id').alias('song_id'),
                                f.col('song_df.artist_id').alias('artist_id'),
                                f.col('df.sessionId').alias('session_id'),
                                f.col('df.location').alias('location'), 
                                f.col('df.userAgent').alias('user_agent'),
                                year('df.datetime').alias('year'),
                                month('df.datetime').alias('month')) \
                                .withColumn('songplay_id', f.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(output_data + '/songplays/songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend/sparkify-lake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
