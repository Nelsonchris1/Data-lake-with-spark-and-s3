import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a saprk session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to read in and process song data
    
    spark: sparkSession
    input_data: file path. read data from s3 bucket
    output_data: file path. write data to s3 bucket
    """
    # get filepath to song data file
    song_data = input_data + "/song-data/A/A/A/*.json"
    
    # read song data file
    print('Loading data!!!')
    df = spark.read.json(song_data)
    print('Data loaded!!')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates() 
    songs_table.createOrReplaceTempView("songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("songs.parquet complete")
    

    # extract columns to create artists table
    print("Loading artist data")
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
                             .withColumnRenamed('artist_name' , 'name') \
                             .withColumnRenamed('artist_location', 'location') \
                             .withColumnRenamed('artist_latitude', 'latitude') \
                             .withColumnRenamed('artist_longitude', 'longitude') \
                             .dropDuplicates()
    
    print("artist_data loaded")
    artists_table.createOrReplaceTempView('artists')
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("artist.parquet complete")
    print("==" * 20)
    

def process_log_data(spark, input_data, output_data):
    """
    Function to read in and process log data
    
    spark: sparkSession
    input_data: file path. read data from s3 bucket
    output_data: file path. write data to s3 bucket
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    print("log data read Successful!!!")
    
    # filter by actions for song plays
    songplay_df = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')

    # extract columns for users table
    print("Extracting user_table")
    user_table = df.select("userId", "firstName", "lastName", "gender", "level")
    user_table.createOrReplaceTempView('users')
    print("user_table extracted")
    
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    print("user parquet completed")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    songplay_df = songplay_df.withColumn('timestamp', get_timestamp(songplay_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    songplay_df = songplay_df.withColumn('datetime', get_datetime(songplay_df.ts)) 
    
    # extract columns to create time table
    print("Extracting time table")
    time_table = songplay_df.select('datetime') \
                           .withColumn('start_time', songplay_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')).dropDuplicates()
    print("time table extracted")
    
#     # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,
                                          'time.parquet'), 'overwrite')
    print("time parquet completed")

    # read in song data to use for songplays table
    song_data = input_data + "/song-data/A/A/A/*.json"
    song_df = spark.read.json(song_data)
    

    # extract columns from joined song and log datasets to create songplays table
    songplay_df = songplay_df.alias('log_df')
    song_df = song_df.alias('song_df')
    joined_df = songplay_df.join(song_df, col('log_df.artist') == col(
        'song_df.artist_name'), 'inner')
    print("creating songplays")
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table.createOrReplaceTempView('songplays')
    print("songplays created")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data,'songplays.parquet'), 'overwrite')
    print("songplays.parquet completed")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
#   output_data = "home/workspace/songs/"
    output_data = "s3a://songdb/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
