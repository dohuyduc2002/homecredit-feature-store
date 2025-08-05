from feast import (
    Field,
    FileSource,
    KafkaSource
)
from feast.types import Int64, Float32
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

application_source = SparkSource(
    name=source_name,
    path=minio_path,
    file_format="csv",
    timestamp_field="updated",
    created_timestamp_column="created",
    description="user application credit source",
    owner="dohuyduc.work@gmail.com",
)

# A push source is useful if you have upstream systems that transform features (e.g. stream processing jobs)
credit_card_balance_push_source = KafkaSource(
    name="credit_card_balance_kafka_source",
    timestamp_field="event_timestamp",
    message_format=JsonFormat(),  # Or your format, e.g., AvroFormat(), ProtobufFormat()
    bootstrap_servers="kafka:9092",   # Update this to your broker
    topic="credit-card-balance-topic",
    # Optionally add batch_source, description, etc.
)
