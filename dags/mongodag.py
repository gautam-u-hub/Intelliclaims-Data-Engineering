from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from user_encryption import update_and_save_user_dataset
from export_data import fun

from datetime import datetime, timedelta
from dags.claims_Data_Quality_Check import data_quality_checks
from user_data_quality_check import u_data_quality_checks
from dags.policies_data_checks import p_data_quality_checks
from dataset_creation import update_and_save_policy_dataset


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 19),
    'retries': 0,
}
dag = DAG(
    'mongo_fetch_and_perform',
    default_args=default_args,
    description='Export MongoDB collections and perform',
    schedule_interval='@yearly',  
)
export_task = PythonOperator(
    task_id='export_collections_to_big_data_formats',
    python_callable=fun,
    dag=dag,
)
with TaskGroup("data_quality_checks", dag=dag) as data_quality_checks_group:
    claims_data_quality_checks = PythonOperator(
        task_id='data_quality_checks_for_claims',
        python_callable=data_quality_checks,
        dag=dag
    )
    user_data_quality_checks = PythonOperator(
        task_id='data_quality_checks_for_users',
        python_callable=u_data_quality_checks,
        dag=dag
    )
    policy_data_quality_checks=PythonOperator(
        task_id='data_quality_checks_for_policies',
        python_callable=p_data_quality_checks
    )
create_dataset=PythonOperator(
    task_id='create_a_dataset',
    python_callable=update_and_save_policy_dataset,
    dag=dag,
)
encrypt_user=PythonOperator(
    task_id='user_data_encryption',
    python_callable=update_and_save_user_dataset,
    dag=dag,
)
export_task >> data_quality_checks_group >> create_dataset >> encrypt_user