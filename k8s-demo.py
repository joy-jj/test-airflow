import pendulum
import time

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.example_dags.libs.helper import print_stuff
from kubernetes.client import models as k8s


k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": 0.5, "memory": "200Mi", "ephemeral-storage": "1Gi"},
                        limits={"cpu": 0.5, "memory": "200Mi", "ephemeral-storage": "1Gi"}
                    )
                )
            ]
        )
    )
}

with DAG(
    dag_id="k8s-demo",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False
):

    @task(executor_config=k8s_exec_config_resource_requirements)
    def resource_requirements_override_example():
        print_stuff()
        print("Hello, World!")
        time.sleep(3600)
  
    resource_requirements_override_example()