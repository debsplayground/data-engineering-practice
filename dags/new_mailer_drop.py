from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import csv
import io
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def drop_new_mailer_file():
    new_mailer_records = [
        ['mail_id','campaign_id','campaign_name','mail_date','first_name','last_name','address','city','state','zip_code','plan_type','market'],
        ['MAIL0101','CMP004','2024 MAPD Spring','2024-03-01','Kevin','Turner','999 New Street','Austin','TX','78701','MAPD','Austin'],
        ['MAIL0102','CMP004','2024 MAPD Spring','2024-03-01','Amanda','Clark','888 Unknown Ave','Phoenix','AZ','85001','PDP','Phoenix'],
        ['MAIL0103','CMP004','2024 MAPD Spring','2024-03-01','Steven','Lewis','777 Random Road','Chicago','IL','60601','MAPD','Chicago'],
        ['MAIL0104','CMP004','2024 MAPD Spring','2024-03-01','Nancy','Scott','111 Organic Way','Denver','CO','80201','MAPD','Denver'],
        ['MAIL0105','CMP004','2024 MAPD Spring','2024-03-01','Paul','Adams','222 Direct Street','Seattle','WA','98101','PDP','Seattle'],
    ]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(new_mailer_records)
    csv_content = output.getvalue()

    # Keys loaded from environment variables - NEVER hardcode keys
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name="us-east-1")
    filename = "mailer_CMP004_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"
    s3.put_object(Bucket="my-databricks-practice-data", Key="dropzone/" + filename, Body=csv_content.encode("utf-8"))
    print("New mailer file dropped into S3 dropzone")
    print("File: dropzone/" + filename)

def confirm_file_landed():
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name="us-east-1")
    response = s3.list_objects_v2(Bucket="my-databricks-practice-data", Prefix="dropzone/")
    if "Contents" in response:
        print("Files in S3 dropzone:")
        for obj in response["Contents"]:
            print("  " + obj['Key'] + "  " + str(round(obj['Size']/1024, 1)) + " KB")
    else:
        print("No files in dropzone yet")

def trigger_matchback_pipeline():
    print("New mailer file detected in dropzone")
    print("Triggering matchback pipeline")
    print("Step 1: Move file from dropzone to raw folder")
    print("Step 2: Load into mail_src Delta table")
    print("Step 3: Run 16 pass matchback against new applications")
    print("Step 4: Save results to apps_matches table")
    print("Step 5: Push results back to TDV for reporting")

with DAG(
    dag_id='simulate_anderson_mailer_drop',
    default_args=default_args,
    description='Simulates Anderson dropping new mailer file into S3',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['matchback', 'mailer', 'ingestion']
) as dag:

    t1 = PythonOperator(task_id='drop_new_mailer_file', python_callable=drop_new_mailer_file)
    t2 = PythonOperator(task_id='confirm_file_landed_in_s3', python_callable=confirm_file_landed)
    t3 = PythonOperator(task_id='trigger_matchback_pipeline', python_callable=trigger_matchback_pipeline)

    t1 >> t2 >> t3
