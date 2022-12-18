from os.path import join
import sys
from datetime import datetime
from airflow.models import DAG
# from airflow.operators.extract import ExtractOperator
from operators.extract_operator import ExtractOperator

#!pip install apache-airflow-providers-apache-spark
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "andre pressendo",
    "depends_on_past": False,
    "start_date": days_ago(6),

}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="dag_processo_vendas", 
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    
    vendedores = ExtractOperator(
        query="Vendedores",
        task_id="vendedores",
        url="https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089815/d0dc9130-6c21-11ed-b5b6-af1378b9873a/vendedores.json",
        file="vendedores",
        file_path=join(
            "/home/andre/Documents/airflow/datalake/dim_vendas/bronze",
            #"vendedores",
            "vendedores.json"
        )
    )

    produtos = ExtractOperator(
        query="Produtos",
        task_id="produtos",
        url="https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089802/d0d7fd50-6c21-11ed-b062-6369d69cae2d/produtos.json",
        file="produtos",
        file_path=join(
            "/home/andre/Documents/airflow/datalake/dim_vendas/bronze",
            #"produtos",
            "produtos.json"
        )
    )

    json_transform = SparkSubmitOperator(
        task_id="transform_json",
        application="/home/andre/Documents/airflow/spark/transformation.py",
        name="transform_json",
        application_args=[
            "--src",
            "/home/andre/Documents/airflow/datalake/dim_vendas/bronze",
            "--dest",
            "/home/andre/Documents/airflow/datalake/dim_vendas/silver"
        ]
    )

    json_insights = SparkSubmitOperator(
        task_id="insights_json",
        application="/home/andre/Documents/airflow/spark/insights.py",
        name="insights_json",
        application_args=[
            "--src",
            "silver",
            "--dest",
            "gold"
        ]
    )

    [vendedores, produtos] >> json_transform >> json_insights
if __name__=="__main__":
    #print('Paths: ', sys.path)
    print(ExtractOperator)