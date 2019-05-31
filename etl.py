import configparser
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY']) = config['AWS'].values()


def create_spark_session():
    """
    Description: Creates the spark session

    Arguments:
        None

    Returns:
        The Spark Session
    """   
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def create_songs_table(spark, input_data):
    """
    Description: Creates songs table

    Arguments:
        spark: the SparkSession
        input_data: path to song data in s3

    Returns:
        Populates s3 with songs table in parquet format
    """   
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format("parquet").save("s3a://udacity-project-4-dim-tables/songs.parquet")
    
    
def create_artists_table(spark, input_data):
    """
    Description: Creates artists table

    Arguments:
        spark: the SparkSession
        input_data: path to song data in s3

    Returns:
        Populates s3 with artists table in parquet format
    """   
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id",                    \
                                  "artist_name as name",           \
                                  "artist_location as location",   \
                                  "artist_latitude as lattitude",  \
                                  "artist_longitude as longitude"])
    
    # write artists table to parquet files
    artists_table.write.format("parquet").save("s3a://udacity-project-4-dim-tables/artists.parquet")
    
    
def create_users_table(spark, input_data):
    """
    Description: Creates users table

    Arguments:
        spark: the SparkSession
        input_data: path to log data in s3

    Returns:
        Populates s3 with user table in parquet format
    """    
    
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # extract columns to create users table
    users_table_df = df.selectExpr([ "userId as user_id",       \
                                  "firstName as first_name",    \
                                  "lastName as last_name",      \
                                  "gender",                     \
                                  "level",                      \
                                  "ts"])
    
    users_max_ts_df = users_table_df                            \
                        .groupBy("user_id")                     \
                        .max("ts")                              \
                        .withColumnRenamed("user_id", "userId") \
                        .withColumnRenamed("max(ts)", "max_ts") \
    
    users_table_join = users_max_ts_df.join                                                           \
                                        (  users_table_df,                                            \
                                           (users_max_ts_df.userId == users_table_df.user_id) &       \
                                           (users_max_ts_df.max_ts == users_table_df.ts)              \
                                        )
    
    users_table = users_table_join.select("user_id", "first_name", "last_name", "gender", "level")
    
    users_table.write.format("parquet").save("s3a://udacity-project-4-dim-tables/users.parquet")


@udf(MapType(StringType(),StringType()))
def extract_fields_from_ts(timestamp):
    dt = pd.to_datetime(timestamp, unit='ms')
    return {
        "start_time" : str(dt),
        "hour"       : str(dt.hour),
        "day"        : str(dt.day),
        "week"       : str(dt.weekofyear),
        "month"      : str(dt.month),
        "year"       : str(dt.year),
        "weekday"    : str(dt.weekday())
    }
    
    
def create_time_table(spark, input_data):
    """
    Description: Creates time table

    Arguments:
        spark: the SparkSession
        input_data: path to log data in s3

    Returns:
        Populates s3 with time table in parquet format
    """    

    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # call UDF on the timestamp
    time_table_df = df.withColumn("time_data", extract_fields_from_ts("ts"))
    
    time_fields = ["start_time", "hour","day", "week", "month", "year", "weekday"]
    exprs = [ "time_data['{}'] as {}".format(field,field) for field in time_fields]
    
    # extract the fields we need for the time table
    time_table = time_table_df.selectExpr(*exprs)
    
    # store the table
    time_table.write.format("parquet").partitionBy("year", "month").save("s3a://udacity-project-4-dim-tables/time.parquet")
    

def create_song_plays_table(spark, input_song_data, input_log_data):
    """
    Description: Loads log & song data from S3 and songplays table

    Arguments:
        spark: the SparkSession
        input_song_data: path to song data in s3
        input_log_data: path to log data in s3

    Returns:
        Populates s3 with fact table in parquet format
    """
    
    # read log data file
    log_df = spark.read.json(input_log_data)
    
    # read song data file
    song_df = spark.read.json(input_song_data)
    
    # join both the dataframes by song title and artist
    combined_df = log_df.join(                                            \
                                song_df,                                  \
                                (log_df.song == song_df.title) &          \
                                (log_df.artist == song_df.artist_name)    \
                             )                                            \
                        .filter(log_df.page=="NextSong")
    
    # create a logic for songplay_id
    temp_table = combined_df.withColumn("songplay_id", monotonically_increasing_id())
    
    # create final table
    songplays_table = temp_table.selectExpr(["songplay_id",                 \
                                             "ts as start_time",            \
                                             "userId as user_id",           \
                                             "level",                       \
                                             "song_id",                     \
                                             "artist_id",                   \
                                             "sessionId as session_id",     \
                                             "artist_location as location", \
                                             "userAgent as user_agent"])
    
    # store the table
    songplays_table.write.format("parquet").save("s3a://udacity-project-4-dim-tables/songplays.parquet")
    
    
def process_song_data(spark, input_data):
    """
    Description: Loads song data from S3 and creates songs and artists tables

    Arguments:
        spark: the SparkSession
        input_data: path to song data in s3

    Returns:
        Populates s3 with dimension tables in parquet format
    """
    
    create_songs_table(spark, input_data)
    create_artists_table(spark, input_data)
    
    
def process_log_data(spark, input_data):
    """
    Description: Loads log data from S3 and creates users and time tables

    Arguments:
        spark: the SparkSession
        input_data: path to log data in s3

    Returns:
        Populates s3 with dimension tables in parquet format
    """
    
    create_users_table(spark, input_data)
    create_time_table(spark, input_data)


def main():
    """
    Description: Loads data from S3 and creates dimesion tables

    Arguments:
        None

    Returns:
        Populates s3 with dimension tables in parquet format
    """
    
    spark = create_spark_session()
    
    input_song_data = "s3a://udacity-dend/song-data/*/*/*/*.json"
    input_log_data  = "s3a://udacity-dend/log_data/*/*/*.json"
    
    process_song_data(spark, input_song_data)    
    process_log_data(spark, input_log_data)
    create_song_plays_table(spark, input_song_data, input_log_data)


if __name__ == "__main__":
    main()
