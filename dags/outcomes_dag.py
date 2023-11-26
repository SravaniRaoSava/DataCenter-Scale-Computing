import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


# code_path = "/root/demo/lab03/etl_scripts"
# sys.path.insert(0, code_path)

from etl_scripts.transform import transform_data
from etl_scripts.ExtractDataFromAPItoGCS import main1
from etl_scripts.LoadDataToPostgres import load_data_to_postgres_main


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="outcomes_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        extract_api_data_to_gcs =  PythonOperator(task_id = "EXTRACT_API_DATA_TO_GCS",
                                                  python_callable = main1,)

        transform_data_step = PythonOperator(task_id="TRANSFORM_DATA",
                                             python_callable=transform_data,)

        load_dim_animals = PythonOperator(task_id="LOAD_DIM_ANIMALS",
                                             python_callable=load_data_to_postgres_main,
                                             op_kwargs={"file_name": 'dim_animal.csv', "table_name": 'dim_animals'},)

        load_dim_outcome_types = PythonOperator(task_id="LOAD_DIM_OUTCOME_TYPES",
                                             python_callable=load_data_to_postgres_main,
                                             op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'dim_outcome_types'},)
        
        load_dim_dates = PythonOperator(task_id="LOAD_DIM_DATES",
                                             python_callable=load_data_to_postgres_main,
                                             op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'dim_dates'},)
        
        load_fct_outcomes = PythonOperator(task_id="LOAD_FCT_OUTCOMES",
                                             python_callable=load_data_to_postgres_main,
                                             op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'fct_outcomes'},)
        
        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_api_data_to_gcs >> transform_data_step >> [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        #start >> transform_data_step >> [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        