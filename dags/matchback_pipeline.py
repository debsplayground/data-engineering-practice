from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def ingest_s3_to_unity_catalog():
    print("STEP 1: Reading Anderson mailer file from Zilverton S3")
    print("Applying schema validation and inference")
    print("Writing to Unity Catalog RAW table:")
    print("usm_dev.aae_gov_marketing.mail_src_raw")

def run_etl_cleaning_job():
    print("STEP 2: Reading from Unity Catalog RAW table")
    print("Cleaning and standardizing data:")
    print("  - Uppercase all names and addresses")
    print("  - Remove duplicates")
    print("  - Validate ZIP codes")
    print("  - Flag data quality issues")
    print("Writing to Unity Catalog CLEANED table:")
    print("usm_dev.aae_gov_marketing.mail_src_updt")

def run_applications_matchback():
    print("STEP 3: Reading cleaned data from Unity Catalog")
    print("Running 16 pass matchback against sfdc_application")
    print("Writing results to:")
    print("usm_dev.aae_gov_marketing.apps_matches")

def run_leads_matchback():
    print("STEP 4: Running 16 pass matchback against sfdc_lead")
    print("Writing results to:")
    print("usm_dev.aae_gov_marketing.leads_matches")

def write_to_oss():
    print("STEP 5: Reading match results from Unity Catalog")
    print("Formatting data for OSS requirements")
    print("Writing final dataset to OSS")

def validate_record_counts():
    print("STEP 6: Validating record counts at each stage")
    print("Comparing output to legacy pipeline")
    print("Confirming data quality and completeness")

with DAG(
    dag_id='anderson_mailer_modernized_pipeline',
    default_args=default_args,
    description='Modernized Anderson mailer pipeline - Zilverton S3 to Unity Catalog to OSS',
    schedule='0 15 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['matchback', 'marketing', 'anderson', 'unity-catalog']
) as dag:

    t1 = PythonOperator(task_id='ingest_s3_to_unity_catalog',   python_callable=ingest_s3_to_unity_catalog)
    t2 = PythonOperator(task_id='run_etl_cleaning_job',          python_callable=run_etl_cleaning_job)
    t3 = PythonOperator(task_id='run_applications_matchback',    python_callable=run_applications_matchback)
    t4 = PythonOperator(task_id='run_leads_matchback',           python_callable=run_leads_matchback)
    t5 = PythonOperator(task_id='write_to_oss',                  python_callable=write_to_oss)
    t6 = PythonOperator(task_id='validate_record_counts',        python_callable=validate_record_counts)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
