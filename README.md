# Data Lake with s3 and Pyspark
_______________________________________________________________________

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Source Data
The datasets are extracted from an s3 buckekts. Here are the links for each of them 
* Song data : `s3://udacity-dend/song_data`
* Log data : `s3://udacity-dend/log_data`

Both datasets have json extentions


## Requirements 
* `Python3`
* `Spark`
* > Access_key_id = ***********
  > Acess_secret_key = ********
  > NOTE!! Dont make this public, always hash your key and secret key wehn upoading to a public repository

## Data Model/Schema 
The table schema adopted here is the start table with Facts and dimension table as follows, 

### Dimension Tables
1. `users` - users in the app (resides in log database)
    * user_id, first_name, last_name, gender, level
    
2. `songs` - songs in music database (resides in song database)
    * songs_id, title, artist_id, year, duration
    
3. `artists` - artist in music database(resides in song database)
    * artist_id, name, location, latitude, longitude
    
4. time - timestamps of records in songplays broken down in units (resides in log database)
    * start_time , hour, day, week, month, year, weekday
    
### Fact Table    
1. `songplays` - records in log data assosicated with songs plays i.e records with page `Next Song`
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
<img src="star_schema_photo.jpg" alt="drawing" width="400"/>


## Project Template

* `etl.py` reads data from an s3 bucket, processes the data using Spark, writes back to an s3 bucket.

* `dl.cfg` contains your AWS credentials
    > NOTE! without this , you cant read or write to an S3 bucket

* `README.md` provides detailed description on your process

