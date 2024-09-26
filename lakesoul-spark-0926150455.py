from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from airflow.kubernetes.secret import Secret
from airflow import DAG
from airflow.utils.dates import days_ago

args = {
    "project_id": "lakesoul-spark-0926150455",
}

dag = DAG(
    "lakesoul-spark-0926150455",
    default_args=args,
    schedule_interval="@once",
    start_date=days_ago(1),
    description="""
Created with Elyra 3.15.0 pipeline editor using `lakesoul-spark.ipynb`.
    """,
    is_paused_upon_creation=False,
)


# Operator source: workspace/lakesoul-spark.ipynb

op_95b514a8_d136_4092_b49d_ccf998c2d0f9 = KubernetesPodOperator(
    name="lakesoul_spark",
    namespace="airflow",
    image="swr.ap-southeast-3.myhuaweicloud.com/dmetasoul-repo/jupyter:v1.0.1",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v3.15.0/elyra/airflow/bootstrapper.py' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.15.0/elyra/airflow/bootstrapper.py --output bootstrapper.py && echo 'Downloading https://raw.githubusercontent.com/elyra-ai/elyra/v3.15.0/etc/generic/requirements-elyra.txt' && curl --fail -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.15.0/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --pipeline-name 'lakesoul-spark' --cos-endpoint http://obs.my-kualalumpur-1.alphaedge.tmone.com.my --cos-bucket obs-lakeinsight-ambank --cos-directory 'lakesoul-spark-0926150455' --cos-dependencies-archive 'lakesoul-spark-95b514a8-d136-4092-b49d-ccf998c2d0f9.tar.gz' --file 'workspace/lakesoul-spark.ipynb' "
    ],
    task_id="lakesoul_spark",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "lakesoul-spark-{{ ts_nodash }}",
    },
    volumes=[],
    volume_mounts=[],
    secrets=[
        Secret("env", "AWS_ACCESS_KEY_ID", "aws-secret-pipeline", "AWS_ACCESS_KEY_ID"),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "aws-secret-pipeline",
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
    annotations={},
    labels={},
    tolerations=[],
    in_cluster=True,
    config_file="None",
    dag=dag,
)

op_95b514a8_d136_4092_b49d_ccf998c2d0f9.image_pull_policy = "IfNotPresent"
