import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocDeleteClusterOperator,
   DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "dataproc_youtube_data_scrape_v2"
PROJECT_ID = "simplesparkjob"
CLUSTER_NAME =  "simplesparkjob-airflow-cluster-v2"
REGION = "us-east1"
JOB_FILE_URI = "gs://simplesparkjob/CodeFile/Scrape_youtube_data_v1.py"
STORAGE_BUCKET = "simplesparkjob"
PIP_INSTALL_PATH = f"gs://goog-dataproc-initialization-actions-{REGION}/python/pip-install.sh"

YESTERDAY = datetime.now() - timedelta(days=1)

default_dag_args = {
    'depends_on_past': False,
    'start_date': YESTERDAY,
}

# Cluster definition 
# You can use the below config if you dont have initialization Actions

# CLUSTER_CONFIG = {
#    "master_config": {
#        "num_instances": 1,
#        "machine_type_uri": "n2-standard-2",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
#    },
#    "worker_config": {
#        "num_instances": 2,
#        "machine_type_uri": "n2-standard-2",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
#    },
# }

# Cluster Config for Dataproc Cluster

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    num_workers=2,
    storage_bucket=STORAGE_BUCKET,
    num_masters=1,
    master_machine_type="n2-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=50,
    worker_machine_type="n2-standard-2",
    worker_disk_type="pd-standard",
    worker_disk_size=50,
    properties={},
    image_version="2.1-ubuntu20",
    autoscaling_policy=None,
    idle_delete_ttl=1800,
    metadata={"PIP_PACKAGES": 'apache-airflow apache-airflow-providers-google google-api-python-client google-auth-oauthlib google-auth-httplib2'},
    init_actions_uris=[
                    PIP_INSTALL_PATH
                ],
).make()


# Pyspark Job submit definition
PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
   }


# DAG to create the Cluster
with DAG(
    DAG_ID,
    schedule="@once",
    default_args=default_dag_args,
    description='A simple DAG to create a Dataproc workflow',
   ) as dag:

   # [START how_to_cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME,

   )

   # [Submit a job to the cluster]
   pyspark_task = DataprocSubmitJobOperator(
       task_id="pyspark_task", 
       job=PYSPARK_JOB, 
       region=REGION, 
       project_id=PROJECT_ID
   )


   # [Delete the cluster after Job Ends]
   delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )



   create_cluster >>  pyspark_task >> delete_cluster