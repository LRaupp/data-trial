from datetime import datetime, timedelta

from scripts.spark_jobs import processors as P
from scripts.clever_main_pipeline import upload_to_postgres

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "alec.ventura",
    "start_date": datetime(2024, 10, 1),
}

datasets = [
    'fmcsa_complaints.csv',
    'fmcsa_safer_data.csv',
    'fmcsa_company_snapshot.csv',
    'fmcsa_companies.csv',
    'customer_reviews_google.csv',
    'company_profiles_google_maps.csv'

]

jobs_processors = [
    P.FMCSACompaniesProcessor(),
    P.FMCSACompanySnapshotProcessor(),
    P.FMCSAComplaintsProcessor(),
    P.GoogleReviewsProcessor(),
    P.GoogleMapsCompanyProfilesProcessor()
]

# The DAG is fiexed and functioning, but it does not adhere to the processing standards needed for the dashboard defined in the Spark DAG
with DAG(
    "clever_main_DAG", 
    default_args=default_args, 
    catchup=False, 
    schedule_interval='20 0 * * *', 
    max_active_runs=1,
    is_paused_upon_creation=True # Paused to use clever_spark_DAG instead
) as dag:

    start_task = EmptyOperator(task_id='Start', dag=dag)
    finish_task = EmptyOperator(task_id='Finish', dag=dag)

    for file in datasets:
        file_without_extension = file.split('.')[0]

        task_id = f"upload_to_postgres_{file_without_extension}"
        upload_to_postgres_task = PythonOperator(
            task_id=task_id,
            python_callable=upload_to_postgres,
            dag=dag,
            execution_timeout=timedelta(seconds=20),
            op_kwargs={
                "file_name": file
            }
        )

        start_task.set_downstream(upload_to_postgres_task)
        upload_to_postgres_task.set_downstream(finish_task)


# This runs the Spark jobs in the same environment as Airflow. 
# My idea is to explore the use of Pandas for data analysis and Spark for preprocessing.
# In a real scenario, these jobs would be executed on a cluster using operators like DatabricksRunNowOperator, EmrAddStepsOperator, etc.
with DAG(
    "clever_spark_DAG", 
    start_date=datetime(2024, 11, 5),
    catchup=False, 
    schedule_interval='20 0 * * *', 
    max_active_runs=1
) as dag:
    
    # It may take some time to download the packages from the Maven repository during the first execution.
    start_task = PythonOperator(task_id='Start', python_callable=P.BaseProcessor.start_spark_session, dag=dag)
    finish_task = PythonOperator(task_id='Finish', python_callable=P.BaseProcessor.stop_spark_session, dag=dag)

    for processor in jobs_processors:
        task_id = f"upload_to_postgres_{processor.target_table_name}"
        upload_to_postgres_task = PythonOperator(
            task_id=task_id,
            python_callable=processor.run,
            dag=dag,
        )

        start_task.set_downstream(upload_to_postgres_task)
    
    upload_to_postgres_task.set_downstream(finish_task)