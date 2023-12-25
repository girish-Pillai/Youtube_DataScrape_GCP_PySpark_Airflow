import os

import json
from datetime import datetime
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import current_date
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
   
def main():
    spark = SparkSession.builder.appName("YoutubeData").getOrCreate()
    
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    
    ##########################################################################
    ##Getting data from search.list() API
    ##########################################################################
    def search_data():
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        # os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

        api_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'  #Use your API key here
        api_service_name = "youtube"
        api_version = "v3"

        # Get credentials and create an API client
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)

        request = youtube.search().list(
            part="snippet",
            channelType="any",
            maxResults=50,
            q="Airflow using GCP",
            order="relevance",
            videoDefinition="any"
        )
        response = request.execute()

        return response

    search_data = search_data()
    
    ##########################################################################
    ##Getting data from video.list() API
    ##########################################################################
    def video_data(video_id):
        api_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'  #Use your API key here
        api_service_name = "youtube"
        api_version = "v3"

        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)


        request = youtube.videos().list(
            part = "snippet",
            id = video_id
        )

        response = request.execute()

        return response
    
    ##########################################################################
    ##Getting description from video.list() API
    ##########################################################################
    def get_description(search_data):
        desc_data = []
        for item in search_data['items']:
            try:
                video_id = item['id']['videoId']
                json_data = video_data(str(video_id))
                description = json_data['items'][0]['snippet']['description']
                tags = json_data['items'][0]['snippet']['tags']
                desc_data.append({'videoId': video_id,
                                  'description': description,
                                  'tags' : tags
                                })
            except KeyError:
                pass

        return desc_data
    
    video_descriptions = get_description(search_data)
        
        
    ##########################################################################
    ##Getting description from video.list() API
    ##########################################################################
    def video_data_from_search(search_data):
        video_data_from_search = []
        for item in search_data['items']:
            try:
                video_id = item['id']['videoId']
            except KeyError:
                video_id = None

            try:
                title = item['snippet']['title']
            except KeyError:
                title = None

            try:
                channel_title = item['snippet']['channelTitle']
            except KeyError:
                channel_title = None

            try:
                published_at = item['snippet']['publishedAt']
            except KeyError:
                published_at = None

            video_data_from_search.append({
                'videoId': video_id,
                'title': title,
                'channelTitle': channel_title,
                'publishedAt': published_at
                # Add more fields as needed
            })
            
        return video_data_from_search
        
    video_data_from_search = video_data_from_search(search_data)
    
    
    ##########################################################################
    ##Appying Spark Transformation
    ##########################################################################
    
    def spark_transformation(video_data_from_search, video_descriptions):

        search_schema = StructType([
            StructField('videoId', StringType(), True),
            StructField('title', StringType(), True),
            StructField('channelTitle', StringType(), True),
            StructField('publishedAt', StringType(), True)
        ])

        desc_schema = StructType([
            StructField('videoId', StringType(), True),
            StructField('description', StringType(), True),
            StructField('tags', ArrayType(StringType()), True)
        ])

        video_df = spark.createDataFrame(video_data_from_search, schema=search_schema)
        desc_df = spark.createDataFrame(video_descriptions, schema=desc_schema)

        spark_df = video_df.join(desc_df, 'videoId', 'inner')

        # Convert 'publishedAt' column to timestamp
        spark_df = spark_df.withColumn('publishedAt', to_timestamp('publishedAt', 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))

        # Adding 'date_created' column with today's date
        spark_df = spark_df.withColumn('date_created', current_date())
        
        output_path = "gs://simplesparkjob/Output_Data/Youtube_Data/"  # Replace with your desired output path

        # Write the DataFrame to parquet table
        spark_df.write \
                .mode("append") \
                .partitionBy("date_created") \
                .parquet(output_path)
        
        print("file written to output_path")
        
    spark_transformation(video_data_from_search, video_descriptions)
        
    
if __name__ == "__main__":
    main()