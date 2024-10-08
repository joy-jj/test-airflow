from pendulum import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
default_args = {
    'owner': "admin"
    }
with DAG(
    dag_id="example_kubernetes_pod",
    schedule="@once",
    start_date=datetime(2024, 9, 27),
    default_args=default_args,
) as dag:
    example_kpo = KubernetesPodOperator(
        kubernetes_conn_id="k8s_conn",
        image="hello-world",
        name="airflow-test-pod",
        task_id="task-one",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    example_kpo