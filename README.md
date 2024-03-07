### A Spark Project to fetch Data using Youtube API Using PySpark and create a tables in Google Cloud Storage using Airflow and Google Cloud Dataproc

This project showcases the workflow for fetching data from the YouTube API, performing transformations via PySpark, and storing the processed data in Google Cloud Storage in Parquet format.

## Overview
Data Retrieval and Transformation: The process involves retrieving data from the YouTube API and conducting transformations using PySpark.

Cloud Service Utilization: Google Cloud Dataproc services are utilized to automate the data processing workflow.

## Details
DAG Configuration: The configuration defines parameters such as DAG_ID, PROJECT_ID, CLUSTER_NAME, REGION, JOB_FILE_URI, etc. These settings enable the setup and management of the Dataproc cluster environment.

Cluster Configuration: Configuration settings for the Dataproc cluster are established utilizing the ClusterGenerator class. This includes specifications for workers, machine types, disk sizes, initialization actions, and more.

## Tasks in the DAG:

create_cluster: Task to create a Dataproc cluster leveraging DataprocCreateClusterOperator.

pyspark_task: Task for submitting a PySpark job to the created cluster via DataprocSubmitJobOperator.

delete_cluster: Task to delete the Dataproc cluster after job completion using DataprocDeleteClusterOperator.

Task Dependencies: Task dependencies are set up to ensure sequential execution. 

The create_cluster task is initiated first, followed by pyspark_task, and finally delete_cluster, utilizing the >> operator.
