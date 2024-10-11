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
                    image="swr.ap-southeast-3.myhuaweicloud.com/dmetasoul-repo/jupyter:v1.0.4",
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
    dag_id="k8s-spark-demo",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False
):

    @task(executor_config=k8s_exec_config_resource_requirements)
    def spark_example():
        print("Hello, World!")
        # from pyspark.sql import SparkSession
        # spark = SparkSession \
        #         .builder \
        #         .master('local[1]') \
        #         .appName("data loading for feast") \
        #         .config("spark.executor.instances", "1") \
        #         .config("spark.executor.memory", "1g") \
        #         .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        #         .getOrCreate()
        # spark.sql('show tables').show()

    spark_example()