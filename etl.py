import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    
    """
    the entry point of a spark application
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    - Load data from song_data dataset.
    - Extract columns for songs and artist tables.
    - Write the transformed tables in parquet files and load them to s3.
    - Parameters:
    spark: the spark session that has been created.
    input_data: the path to the song_data s3 bucket.
    output_data: the path where the parquet files will be written.
    """
    
    # get filepath to song data file
    song_data =  input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create song view to write in SQL
    df.createOrReplaceTempView("song_view")

    # extract columns to create songs table
    songs_table =  spark.sql(""" 
    SELECT sv.song_id,
    sv.title,
    sv.artist_id,
    sv.year,
    sv.duration 
    FROM song_view sv 
    WHERE song_id IS NOT NULL 
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql(""" 
    SELECT DISTINCT ar.artist_id,
    ar.artist_name,
    ar.artist_location,
    ar.artist_latitude,
    ar.artist_longitude 
    FROM song_view ar 
    WHERE ar.artist_id IS NOT NULL 
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    
    """
    - Load data from log_data dataset- Extract columns for users and time tables
    - Read both the log_data and song_data datasets. 
    - Extract columns for songplays table with the data.
    - Write the transformed tables in parquet files and load them to s3.
    Parameters: 
    spark: spark session that has been created. 
    input_data: the path to the log_data s3 bucket. 
    output_data: the path where the parquet files will be written.
    """
    
    # get filepath to log data file
    log_data =input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create song view to write in SQL
    df.createOrReplaceTempView("log_view")

    # extract columns for users table    
    users_table = spark.sql(""" 
    SELECT DISTINCT us.userId as user_id,
    us.firstName as first_name,
    us.lastName as last_name,
    us.gender as gender,
    us.level as level 
    FROM log_view us 
    WHERE us.userId IS NOT NULL 
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda i: str(int(int(i)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda j: str(datetime.fromtimestamp(int(j) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime').\
    withColumn('start_time', df.datetime).\
    withColumn('hour', hour('datetime')).\
    withColumn('day', dayofmonth('datetime')).\
    withColumn('week', weekofyear('datetime')).\
    withColumn('month', month('datetime')).\
    withColumn('year', year('datetime')).\
    withColumn('weekday', dayofweek('datetime')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_data =  input_data + 'song_data/A/A/A/*.json'
    song_df = spark.read.json(song_data) 
    
    # join song and log datasets
    joined_df = df.join(song_df, [df.song == song_df.title , df.artist == song_df.artist_name])
    
    joined_df.createOrReplaceTempView("joined_df_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT monotonically_increasing_id() as songplay_id,
    to_timestamp(ts/1000) as start_time,
    month(to_timestamp(ts/1000)) as month,
    year(to_timestamp(ts/1000)) as year,
    userId as user_id,
    level as level,
    song_id as song_id,
    artist_id as artist_id,
    sessionId as session_id,
    location as location,
    userAgent as user_agent 
    FROM joined_df_view 
    """)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    
    """
    - create a spark session.
    - load the song and log data from s3.
    - get the data and transform them to tables
    - write the transformed tables to parquet files.
    - Load the parquet files from s3.
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://abdullahoutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
