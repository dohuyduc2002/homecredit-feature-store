from feast import (
    Field,
    FileSource,
    KafkaSource
)

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


def create_spark_source(spark_src_name, minio_path, file_format, timestamp_field, created_timestamp_column):
    application_source = SparkSource(
        name=spark_src_name,
        path=minio_path,
        file_format=file_format,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        description="user spark source",
        owner="dohuyduc.work@gmail.com",
    )
    return application_source

def create_kafka_source(kafka_src_name, topic, bootstrap_servers, message_format, timestamp_field):
    # A push source is useful if you have upstream systems that transform features (e.g. stream processing jobs)
    credit_card_balance_push_source = KafkaSource(
        name=kafka_src_name,
        timestamp_field=timestamp_field,
        message_format=message_format,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        description="user kafka source",
        owner="dohuyduc.work@gmail.com",
    )
    return credit_card_balance_push_source

