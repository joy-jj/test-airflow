import pendulum
import time

from airflow.models.dag import DAG
from airflow.decorators import task
from kubernetes.client import models as k8s


k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="swr.ap-southeast-3.myhuaweicloud.com/dmetasoul-repo/jupyter:v1.0.5-airflow-v3",
                    command=["/bin/bash", "/opt/run-airflow.sh"],
                    volume_mounts=[
                        k8s.V1VolumeMount(name="ephemeral-volume", mount_path="/home/jovyan")
                    ],
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": 1, "memory": "500Mi", "ephemeral-storage": "1Gi"},
                        limits={"cpu": 1, "memory": "500Mi", "ephemeral-storage": "1Gi"},
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
        )
    )
}


with DAG(
    dag_id="ml-example",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
):
    def get_spark_session():
        from pyspark.sql import SparkSession
        spark = SparkSession \
                .builder \
                .master('local[1]') \
                .appName("data loading for feast") \
                .config("spark.executor.instances", "1") \
                .config("spark.executor.memory", "1g") \
                .config("spark.kubernetes.namespace", "lake-public") \
                .getOrCreate()
        return spark
    
    @task(executor_config=k8s_exec_config_resource_requirements)
    def generate_train_data():
        spark = get_spark_session()
        df = spark.read.csv('aws://obs-lakeinsight-ambank/airflow/winequality-red.csv', header=True, inferSchema=True, sep=';')
        df.printSchema()
        df.show()
        df.write.format("lakesoul").saveAsTable("winequalit_table2")
        
    @task(executor_config=k8s_exec_config_resource_requirements)
    def training():
        from lakesoul.arrow import lakesoul_dataset
        import numpy as np
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import ElasticNet
        from urllib.parse import urlparse
        import mlflow
        import mlflow.sklearn
        from mlflow.models.signature import infer_signature

        import logging

        logging.basicConfig(level=logging.WARN)
        logger = logging.getLogger(__name__)


        def eval_metrics(actual, pred):
            rmse = np.sqrt(mean_squared_error(actual, pred))
            mae = mean_absolute_error(actual, pred)
            r2 = r2_score(actual, pred)
            return rmse, mae, r2
        # spark = get_spark_session()
        ds = lakesoul_dataset("winequalit_table2")
        df = ds.to_table().to_pandas()
        
        train, test = train_test_split(df)

        # The predicted column is "quality" which is a scalar from [3, 9]
        train_x = train.drop(["quality"], axis=1)
        test_x = test.drop(["quality"], axis=1)
        train_y = train[["quality"]]
        test_y = test[["quality"]]

        alpha = 0.5
        l1_ratio = 0.5

        # specify the experiment name here
        with mlflow.start_run(run_name='wine-quality-lr2'):
            lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
            lr.fit(train_x, train_y)

            predicted_qualities = lr.predict(test_x)

            (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

            print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
            print("  RMSE: %s" % rmse)
            print("  MAE: %s" % mae)
            print("  R2: %s" % r2)

            mlflow.log_param("alpha", alpha)
            mlflow.log_param("l1_ratio", l1_ratio)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("r2", r2)
            mlflow.log_metric("mae", mae)

            tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
            model_signature = infer_signature(train_x, train_y)

            # Model registry does not work with file store
            if tracking_url_type_store != "file":

                # Register the model
                # There are other ways to use the Model Registry,
                # which depends on the use case,
                # please refer to the doc for more information:
                # https://mlflow.org/docs/latest/model-registry.html#api-workflow
                mlflow.sklearn.log_model(
                    lr,
                    "model",
                    registered_model_name="ElasticnetWineModel",
                    signature=model_signature,
                )
            else:
                mlflow.sklearn.log_model(lr, "model", signature=model_signature)
        
    generate_train_data()
    # generate_train_data() >> training()
