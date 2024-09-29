from __future__ import annotations

import os
from datetime import datetime

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

# [START howto_operator_k8s_cluster_resources]
secret_file = Secret("volume", "/etc/sql_conn", "airflow-secrets", "sql_alchemy_conn")

secret_env = Secret("env", "SQL_CONN", "airflow-secrets", "sql_alchemy_conn")

secret_all_keys = Secret("env", None, "airflow-secrets-2")

volume_mount = k8s.V1VolumeMount(
    name="test-volume", mount_path="/root/mount_file", sub_path=None, read_only=True
)


configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-1")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-2")),
]


volume = k8s.V1Volume(
    name="test-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
)


port = k8s.V1ContainerPort(name="http", container_port=80)


init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path="/etc/foo", name="test-volume", sub_path=None, read_only=True)
]


init_environments = [k8s.V1EnvVar(name="key1", value="value1"), k8s.V1EnvVar(name="key2", value="value2")]


init_container = k8s.V1Container(
    name="init-container",
    image="ubuntu:16.04",
    env=init_environments,
    # volume_mounts=init_container_volume_mounts,
    command=["bash", "-cx"],
    args=["echo 10"],
)


affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="disktype", operator="In", values=["ssd"])
                    ]
                ),
            )
        ]
    ),
    pod_affinity=k8s.V1PodAffinity(
        required_during_scheduling_ignored_during_execution=[
            k8s.V1WeightedPodAffinityTerm(
                weight=1,
                pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(
                        match_expressions=[
                            k8s.V1LabelSelectorRequirement(key="security", operator="In", values="S1")
                        ]
                    ),
                    topology_key="failure-domain.beta.kubernetes.io/zone",
                ),
            )
        ]
    ),
)


tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]


# [END howto_operator_k8s_cluster_resources]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_kubernetes_operator"


with DAG(
    dag_id="example_kubernetes_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    k = KubernetesPodOperator(
        namespace="airflow",
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        #
        # secrets=[secret_file, secret_env, secret_all_keys],
        ports=[port],
        #
        # volumes=[volume],
        # volume_mounts=[volume_mount],
        # env_from=configmaps,
        
        name="airflow-test-pod",
        task_id="task",
        affinity=affinity,
        on_finish_action="keep_pod",
        hostnetwork=False,
        tolerations=tolerations,
        init_containers=[init_container],
        priority_class_name="medium",
    )


    # # [START howto_operator_k8s_private_image]
    # quay_k8s = KubernetesPodOperator(
    #     namespace="default",
    #     image="quay.io/apache/bash",
    #     image_pull_secrets=[k8s.V1LocalObjectReference("testquay")],
    #     cmds=["bash", "-cx"],
    #     arguments=["echo", "10", "echo pwd"],
    #     labels={"foo": "bar"},
    #     name="airflow-private-image-pod",
    #     on_finish_action="delete_pod",
    #     in_cluster=True,
    #     task_id="task-two",
    #     get_logs=True,
    # )
    # # [END howto_operator_k8s_private_image]

    # # [START howto_operator_k8s_write_xcom]
    # write_xcom = KubernetesPodOperator(
    #     namespace="default",
    #     image="alpine",
    #     cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
    #     name="write-xcom",
    #     do_xcom_push=True,
    #     on_finish_action="delete_pod",
    #     in_cluster=True,
    #     task_id="write-xcom",
    #     get_logs=True,
    # )

    # pod_task_xcom_result = BashOperator(
    #     bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
    #     task_id="pod_task_xcom_result",
    # )

    # write_xcom >> pod_task_xcom_result
    # [END howto_operator_k8s_write_xcom]