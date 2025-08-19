
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable
import logging
import boto3
import pandas as pd
import io

# Define default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='example_etl_dag',
    default_args=default_args,
    description='An example ETL data pipeline using MWAA, S3, and Redshift',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: List files in S3 bucket (source data)
    list_files = S3ListOperator(
        task_id='list_s3_files',
        bucket='{{ var.value.source_s3_bucket }}',
        aws_conn_id='aws_default'
    )

    # Python function to transform data
    def data_transform(**context):
        s3 = boto3.client('s3')
        bucket_name = context['params']['source_bucket']
        key = context['params']['source_key']
        
        # Download file from S3
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        # Simple transformation example: Add a new column
        df['transformed_column'] = df.iloc[:, 0] * 2
        
        # Save transformed data to CSV buffer
        transformed_buffer = io.StringIO()
        df.to_csv(transformed_buffer, index=False)
        
        # Upload transformed data back to S3
        s3.put_object(Bucket=bucket_name, Key='transformed/' + key.split('/')[-1], Body=transformed_buffer.getvalue())
        logging.info(f"Transformed data uploaded to S3 at transformed/{key.split('/')[-1]}")

    # Task 2: Transform the file in S3
    transform_task = PythonOperator(
        task_id='data_transform',
        python_callable=data_transform,
        provide_context=True,
        params={
            'source_bucket': '{{ var.value.source_s3_bucket }}',
            'source_key': '{{ task_instance.xcom_pull("list_s3_files")[0] }}'
        }
    )

    # Task 3: Load data into Redshift
    load_to_redshift = RedshiftSQLOperator(
        task_id='load_to_redshift',
        redshift_conn_id='redshift_default',
        sql="""
        COPY your_table
        FROM 's3://{{ var.value.source_s3_bucket }}/transformed/{{ task_instance.xcom_pull("list_s3_files")[0].split('/')[-1] }}'
        IAM_ROLE '{{ var.value.redshift_iam_role }}'
        CSV
        IGNOREHEADER 1;
        """,
    )

    # Set task dependencies
    list_files >> transform_task >> load_to_redshift
