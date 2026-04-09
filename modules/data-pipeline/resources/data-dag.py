# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago

PROJECT_ID = "{{ var.value.project_id }}"
REGION = "{{ var.value.region_name }}"
BUCKET = "{{ var.value.bucket_name }}"
DATAPROC_CLUSTER = "{{ var.value.phs_cluster }}"
JOB_FILE_URI = "gs://{{ var.value.bucket_name }}/spark-job.py"

# Output path passed as argument to spark-job.py — avoids hardcoding the bucket name
DATA_BUCKET = "gs://{{ var.value.project_id }}-data/data/shakespeare/"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": JOB_FILE_URI,
        "args": [DATA_BUCKET],
        "properties": {
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.executor.instances": "2",
        },
    },
}

default_args = {
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
}

with models.DAG(
    "dataproc_job",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
    )
    pyspark_task
