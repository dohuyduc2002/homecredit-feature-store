from datetime import timedelta
from pathlib import Path
import os
from minio import Minio

import feast.types
from feast import (
    Entity,
    FeatureView,
    Field,
    Project,
    ValueType,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

minio_client = Minio(
    "minio-server.example.com",
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    secure=False,
)

def load_and_save_data(bucket_name, blob_name):
    os.makedirs("data", exist_ok=True)
    local_path = Path("data") / blob_name
    minio_client.fget_object(bucket_name, blob_name, str(local_path))
    return str(local_path)

def create_feast_config():
    project = Project(name="homecredit")
    entity = Entity(
        name="application", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
    )
    return project, entity


def create_feature_source(source_name, minio_path):
    application_source = SparkSource(
        name=source_name,
        path=minio_path,
        file_format="csv",
        timestamp_field="updated",  # Assuming 'updated' is the timestamp field in your CSV
        created_timestamp_column="created",  # Assuming 'created' is the created timestamp field in your CSV
    )
    return application_source


def create_feature_view(entity, source, feature_view_name):
    application_fv = FeatureView(
        name=feature_view_name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="SK_ID_CURR", dtype=feast.types.String),
            Field(name="CNT_CHILDREN", dtype=feast.types.Int32),
            Field(name="AMT_GOODS_PRICE", dtype=feast.types.Float64),
            Field(name="NAME_INCOME_TYPE", dtype=feast.types.String),
            Field(name="OCCUPATION_TYPE", dtype=feast.types.String),
            Field(name="CNT_FAM_MEMBERS", dtype=feast.types.Float64),
            Field(name="EXT_SOURCE_3", dtype=feast.types.Float64),
            Field(name="ENTRANCES_AVG", dtype=feast.types.Float64),
            Field(name="LIVINGAPARTMENTS_AVG", dtype=feast.types.Float64),
            Field(name="APARTMENTS_MODE", dtype=feast.types.Float64),
            Field(name="LIVINGAPARTMENTS_MODE", dtype=feast.types.Float64),
            Field(name="YEARS_BEGINEXPLUATATION_MEDI", dtype=feast.types.Float64),
            Field(name="FLOORSMAX_MEDI", dtype=feast.types.Float64),
            Field(name="NONLIVINGAPARTMENTS_MEDI", dtype=feast.types.Float64),
            Field(name="FONDKAPREMONT_MODE", dtype=feast.types.String),
            Field(name="OBS_30_CNT_SOCIAL_CIRCLE", dtype=feast.types.Float64),
            Field(name="DEF_60_CNT_SOCIAL_CIRCLE", dtype=feast.types.Float64),
            Field(name="FLAG_DOCUMENT_5", dtype=feast.types.Int32),
            Field(name="FLAG_DOCUMENT_12", dtype=feast.types.Int32),
            Field(name="FLAG_DOCUMENT_7", dtype=feast.types.Int32),
            Field(name="FLAG_DOCUMENT_20", dtype=feast.types.Int32),
            Field(name="AMT_REQ_CREDIT_BUREAU_QRT", dtype=feast.types.Float64),
            Field(name="DAYS_EMPLOYED", dtype=feast.types.Int32),
        ],
        online=True,
        source=source,
    )
    return application_fv


# def create_feature_service(feature_service_name, feature_views):
#     transactions_fs = FeatureService(
#         name=feature_service_name,
#         features=[feature_views],
#         logging_config=LoggingConfig(destination=FileLoggingDestination(path="data")),
#     )

# if __name__ == "__main__":
bucket_name = "sample-data"
blob_name = "curated_train_data.csv"
minio_path = f"s3a://minio.minio.svc.cluster.local:9000/{bucket_name}/{blob_name}"
feature_view_name = "application_feature_view"
source_name = "application_source"

project, entity = create_feast_config()
application_source = create_feature_source(source_name, minio_path)
application_fv = create_feature_view(entity, application_source, feature_view_name)
