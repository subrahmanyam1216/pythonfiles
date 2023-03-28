import datetime

# [START composer_notify_failure_airflow_1]
from airflow import models

# [END composer_notify_failure_airflow_1]
from airflow.contrib.operators import bigquery_get_data

# [START composer_bigquery_airflow_1]
from airflow.contrib.operators import bigquery_operator

# [END composer_bigquery_airflow_1]
from airflow.contrib.operators import bigquery_to_gcs

# [START composer_bash_bq_airflow_1]
from airflow.operators import bash_operator

# [END composer_bash_bq_airflow_1]
# [START composer_email_airflow_1]
from airflow.operators import email_operator

# [END composer_email_airflow_1]
from airflow.utils import trigger_rule

#bq_dataset_name = "dev-project-381213" 
bq_dataset_name =  "{{var.value.bq_dataset_name}}"    
bq_recent_questions_table_id = bq_dataset_name + ".test"
BQ_MOST_POPULAR_TABLE_NAME = "test-table"
bq_most_popular_table_id = bq_dataset_name + "." + BQ_MOST_POPULAR_TABLE_NAME
output_file = "gs://{gcs_bucket}/username.csv".format(
    gcs_bucket="{{var.value.gcs_bucket}}"
)


max_query_date = "2018-02-01"
min_query_date = "2018-01-01"

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

# [START composer_notify_failure_airflow_1]
default_dag_args = {
    "start_date": yesterday,
    # Email whenever an Operator in the DAG fails.
    "email": "{{var.value.email}}",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": "{{var.value.gcp_project}}",
}

with models.DAG(
    "composer_sample_bq_notify",
    schedule_interval=datetime.timedelta(weeks=4),
    default_args=default_dag_args,
) as dag:
   
    # START composer_bash_bq_airflow_1
    # Create BigQuery output dataset.
    make_bq_dataset = bash_operator.BashOperator(
        task_id="make_bq_dataset",
        # Executing 'bq' command requires Google Cloud SDK which comes
       
        bash_command="bq ls {} || bq mk {}".format(bq_dataset_name, bq_dataset_name),
       # bq ls --max_results 1000 'project_id:dataset'
       # bq mk --schema name:string,value:integer -t mydataset.newtable
    )
   

    # [START composer_bigquery_airflow_1]
    # Query recent StackOverflow questions.
    bq_recent_questions_query = bigquery_operator.BigQueryOperator(
        task_id="bq_recent_questions_query",
        sql="""
        SELECT Loginemail, First_name, Last_name                     		

        FROM `dev-project-381213.test.test-table`

        SELECT * FROM `dev-project-381213.test.test-table` LIMIT 1000

        ORDER BY ID DESC
        LIMIT 100
        """.format(
            #max_date=max_query_date, min_date=min_query_date
        ),
        use_legacy_sql=False,
        destination_dataset_table=bq_recent_questions_table_id,
    )


    # Export query result to Cloud Storage.
    export_questions_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="export_recent_questions_to_gcs",
        source_project_dataset_table=bq_recent_questions_table_id,
        destination_cloud_storage_uris=[output_file],
        export_format="CSV",
    )

    # Perform most popular question query.
    bq_most_popular_query = bigquery_operator.BigQueryOperator(
        task_id="bq_most_popular_question_query",
        sql="""
        SELECT title, view_count
        FROM `{table}`
        ORDER BY view_count DESC
        LIMIT 1
        """.format(
            table=bq_recent_questions_table_id
        ),
        use_legacy_sql=False,
        destination_dataset_table=bq_most_popular_table_id,
    )

   
    bq_read_most_popular = bigquery_get_data.BigQueryGetDataOperator(
        task_id="bq_read_most_popular",
        dataset_id=bq_dataset_name,
        table_id=BQ_MOST_POPULAR_TABLE_NAME,
    )

 
    # Send email confirmation
    email_summary = email_operator.EmailOperator(
        task_id="email_summary",
        to="{{var.value.email}}",
        subject="Sample BigQuery notify data ready",
        html_content="""
        Analyzed Stack Overflow posts data from {min_date} 12AM to {max_date}
        12AM. The most popular question was '{question_title}' with
        {view_count} views. Top 100 questions asked are now available at:
        {export_location}.
        """.format(
            min_date=min_query_date,
            max_date=max_query_date,
            question_title=(
                "{{ ti.xcom_pull(task_ids='bq_read_most_popular', "
                "key='return_value')[0][0] }}"
            ),
            view_count=(
                "{{ ti.xcom_pull(task_ids='bq_read_most_popular', "
                "key='return_value')[0][1] }}"
            ),
            export_location=output_file,
        ),
    )
   
    # Delete BigQuery dataset
    # Delete the bq table
    delete_bq_dataset = bash_operator.BashOperator(
        task_id="delete_bq_dataset",
        bash_command="bq rm -r -f %s" % bq_dataset_name,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    # Define DAG dependencies.
    (
        make_bq_dataset
        >> bq_recent_questions_query
        >> export_questions_to_gcs
        >> delete_bq_dataset
    )
    (
        bq_recent_questions_query
        >> bq_most_popular_query
        >> bq_read_most_popular
        >> delete_bq_dataset
    )
    export_questions_to_gcs >> email_summary
    bq_read_most_popular >> email_summary