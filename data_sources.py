from feast import KafkaSource

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.data_format import StreamFormat, AvroFormat

import os
from minio import Minio

s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

minio_client = Minio(
    "minio-server.example.com",
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    secure=False,
)


def create_spark_source(spark_src_name, minio_path):
    application_source = SparkSource(
        name=spark_src_name,
        path=minio_path,
        file_format="csv",
        timestamp_field="updated",  
        created_timestamp_column="created",  
        description="user spark source",
        owner="dohuyduc.work@gmail.com",
    )
    return application_source

def create_kafka_source(kafka_src_name, topic, bootstrap_servers, timestamp_field):
    # A push source is useful if you have upstream systems that transform features (e.g. stream processing jobs)
    credit_card_balance_push_source = KafkaSource(
        name=kafka_src_name,
        timestamp_field=timestamp_field,
        message_format=AvroFormat,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        description="user kafka source",
        owner="dohuyduc.work@gmail.com",
    )
    return credit_card_balance_push_source

## Create Spark batch source
spark_src_names = ["application", "previous-application"]
spark_src_paths = ["s3a://sample-data/curated_train_data.csv", "s3a://sample-data/previous_application.csv"]

application_source = create_spark_source(spark_src_names[0], spark_src_paths[0])
previous_application_source = create_spark_source(spark_src_names[1], spark_src_paths[1])


## Create Kafka source
kafka_src_topics = ["bureau-balance", "bureau", "bureau-balance","credit-card-balance","credit-card-balance","installments-payments"]
kafka_src_names = ["bureau-balance", "bureau", "bureau-balance","credit-card-balance","credit-card-balance","installments-payments"]
kafka_bootstrap_servers = ":9092"
kafka_timestamp_field = "updated"