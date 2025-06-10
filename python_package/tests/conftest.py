import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

from etl.config import raw_bucket_name


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("PySpark App")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.375",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def glue_context():
    spark = (
        SparkSession.builder.appName("Iceberg Spark Session")
        .master("local[*]")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.glue_catalog.warehouse", f"s3://{raw_bucket_name}/warehouse")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "glue_catalog")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.aws.profile", "neogen")
        .config("spark.sql.iceberg.write-allow-schema-evolution", "true")
        .config("spark.sql.catalog.glue_catalog.default-namespace", "raw")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.parquet.mergeSchema", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    glue_context = GlueContext(sc)
    return glue_context


# --conf spark.sql.catalog.glue_catalog.default-namespace=stage
