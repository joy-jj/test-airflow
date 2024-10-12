import pendulum
import time

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.example_dags.libs.helper import print_stuff
from kubernetes.client import models as k8s


k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"access-lakesoul": "true"}),
        # metadata=k8s.V1ObjectMeta(labels={"access-lakeinsight": "true"}, namespace="lake-public"),
        spec=k8s.V1PodSpec(
            # service_account_name="lake-public",
            containers=[
                k8s.V1Container(
                    name="base",
                    image="swr.ap-southeast-3.myhuaweicloud.com/dmetasoul-repo/jupyter:v1.0.5-airflow-v4",
                    command=["/bin/bash", "/opt/run-airflow.sh"],
                    volume_mounts=[
                        k8s.V1VolumeMount(name="ephemeral-volume", mount_path="/home/jovyan")
                    ],
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": 1, "memory": "1Gi", "ephemeral-storage": "1Gi"},
                        limits={"cpu": 1, "memory": "1Gi", "ephemeral-storage": "1Gi"},
                    ),
                )
            ],
            volumes=[
                k8s.V1Volume(
                    name="ephemeral-volume",
                    ephemeral=k8s.V1EphemeralVolumeSource(
                        volume_claim_template=k8s.V1PersistentVolumeClaimTemplate(
                            spec=k8s.V1PersistentVolumeClaimSpec(
                                access_modes=["ReadWriteOnce"],
                                storage_class_name="csi-disk",
                                resources=k8s.V1ResourceRequirements(requests={"storage": "10Gi"}),
                            )
                        )
                    ),
                )
            ],
        ),
    )
}


with DAG(
    dag_id="k8s-spark-sql-lakesoul-demo",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
):

    @task(executor_config=k8s_exec_config_resource_requirements)
    def sparksql_example():
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("data loading for feast")
            .config("spark.executor.instances", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.kubernetes.namespace", "lake-public")
            .getOrCreate()
        )
        spark.sql("show tables").show()
        spark.sql("select * from driver_hourly_stats2 limit 3").show()

    sparksql_example()
