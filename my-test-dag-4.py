from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


def get_task_id():
    print("Executing 4")
    return "print_array_task_4"  # <- is that code going to be executed? YES


def get_array():
    print("Executing 4")
    return [1, 2, 3]  # <- is that code going to be executed? NO

default_args = {
    'owner': "admin"
    }
with DAG(
    dag_id="my-test-dag-4",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["my-demo"],
) as dag:
    operator = PythonOperator(
        task_id=get_task_id(),
        python_callable=get_array,
        dag=dag,
    )
