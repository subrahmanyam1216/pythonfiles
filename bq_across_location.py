
# Load The Dependencies


import csv
import datetime
import io
import logging

from airflow import models
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_to_gcs
from airflow.operators import dummy_operator



# Set default arguments


yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "airflow",
    "start_date": yesterday,
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


# Set variables



# Source Bucket
source_bucket = "{{var.value.gcs_source_bucket}}"

# Destination Bucket
dest_bucket = "{{var.value.gcs_dest_bucket}}"


# Set GCP logging


logger = logging.getLogger("bq_copy_us_to_eu_01")


# Functions


def read_table_list(table_list_file):
    """
    Reads the table list file that will help in creating Airflow tasks in
    the DAG dynamically.
    :param table_list_file: (String) The file location of the table list file,
    e.g. '/home/airflow/framework/table_list.csv'
    :return table_list: (List) List of tuples containing the source and
    target tables.
    """
    table_list = []
    logger.info("Reading table_list_file from : %s" % str(table_list_file))
    try:
        with io.open(table_list_file, "rt", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # skip the headers
            for row in csv_reader:
                logger.info(row)
                table_tuple = {"table_source": row[0], "table_dest": row[1]}
                table_list.append(table_tuple)
            return table_list
    except IOError as e:
        logger.error("Error opening table_list_file %s: " % str(table_list_file), e)

# Main DAG


# DAG object.
with models.DAG(
    "composer_sample_bq_copy_across_locations",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    start = dummy_operator.DummyOperator(task_id="start", trigger_rule="all_success")

    end = dummy_operator.DummyOperator(task_id="end", trigger_rule="all_success")
   
    # 'table_list_file_path': This variable will contain the location of the main
  
    table_list_file_path = models.Variable.get("table_list_file_path")

    # Get the table list from main file
    all_records = read_table_list(table_list_file_path)

    # Loop over each record in the 'all_records' python list to build up
    # Airflow tasks
    for record in all_records:
        logger.info("Generating tasks to transfer table: {}".format(record))

        table_source = record["table_source"]
        table_dest = record["table_dest"]

        BQ_to_GCS = bigquery_to_gcs.BigQueryToCloudStorageOperator(
           
            task_id="{}_BQ_to_GCS".format(table_source.replace(":", "_")),
            source_project_dataset_table=table_source,
            destination_cloud_storage_uris=[
                "{}-*.avro".format("gs://" + source_bucket + "/" + table_source)
            ],
            export_format="AVRO",
        )

        GCS_to_GCS = gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator(
            # Replace ":" with valid character for Airflow task
            task_id="{}_GCS_to_GCS".format(table_source.replace(":", "_")),
            source_bucket=source_bucket,
            source_object="{}-*.avro".format(table_source),
            destination_bucket=dest_bucket,
            # destination_object='{}-*.avro'.format(table_dest)
        )

        GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            # Replace ":" with valid character for Airflow task
            task_id="{}_GCS_to_BQ".format(table_dest.replace(":", "_")),
            bucket=dest_bucket,
            source_objects=["{}-*.avro".format(table_source)],
            destination_project_dataset_table=table_dest,
            source_format="AVRO",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        start >> BQ_to_GCS >> GCS_to_GCS >> GCS_to_BQ >> end