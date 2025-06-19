import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from etl.config import datalake_bucket_name


@pytest.fixture(scope="session")
def glue_context():
    spark = (
        SparkSession.builder.appName("Iceberg Spark Session")
        .master("local[*]")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.glue_catalog.warehouse", f"s3://{datalake_bucket_name}/test_datalake")
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


# new_props ="""
#  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160" \
#   --master "local[*]" \
#   --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
#   --conf "spark.sql.defaultCatalog=spark_catalog" \
#    --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" \
#   --conf "spark.sql.catalog.spark_catalog.type=rest" \
#   --conf "spark.sql.catalog.spark_catalog.uri=https://s3tables.<Region>.amazonaws.com/iceberg" \
#   --conf "spark.sql.catalog.spark_catalog.warehouse=arn:aws:s3tables:<Region>:<accountID>:bucket/<bucketname>" \
#   --conf "spark.sql.catalog.spark_catalog.rest.sigv4-enabled=true" \
#   --conf "spark.sql.catalog.spark_catalog.rest.signing-name=s3tables" \
#   --conf "spark.sql.catalog.spark_catalog.rest.signing-region=<Region>" \
#   --conf "spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
#   --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider" \
#   --conf "spark.sql.catalog.spark_catalog.rest-metrics-reporting-enabled=false"
# """

# give me a context based on new_props
region = "us-east-1"
account_id = "664418979226"
s3_bucket_name = "caylent-poc-table-bucket"

@pytest.fixture(scope="session")
def s3_tables_context():
    # spark = (
    #     SparkSession.builder.appName("S3 Tables Spark Session")
    #     .master("local[*]")
    #     .config(
    #         "spark.jars.packages",
    #         "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160",
    #     )
    #     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    #     .config("spark.sql.defaultCatalog", "spark_catalog")
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    #     .config("spark.sql.catalog.spark_catalog.type", "rest")
    #     .config("spark.sql.catalog.spark_catalog.uri", f"https://s3tables.{region}.amazonaws.com/iceberg")
    #     .config("spark.sql.catalog.spark_catalog.warehouse", f"arn:aws:s3tables:{region}:{account_id}:bucket/{s3_bucket_name}")
    #     .config("spark.sql.catalog.spark_catalog.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
    #     .config("spark.sql.catalog.spark_catalog.rest.sigv4-enabled", "true")
    #     .config("spark.sql.catalog.spark_catalog.rest.signing-name", "s3tables")
    #     .config("spark.sql.catalog.spark_catalog.rest.signing-region", f"{region}")
    #     .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    #     #.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider")
    #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    #     .config("spark.sql.catalog.spark_catalog.rest-metrics-reporting-enabled", "false")
    #     .getOrCreate()
    # )

    spark = (SparkSession.builder.appName("S3TablesSparkSession") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars", "/home/hadoop/workspace/python_package/jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar") \
        .config("spark.sql.defaultCatalog","s3tables") \
        .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tables.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        #.config("spark.sql.catalog.s3tables.glue.id", f"{account_id}:s3tablescatalog/{s3_bucket_name}") \
        .config("spark.sql.catalog.s3tables.warehouse", f"arn:aws:s3tables:{region}:{account_id}:bucket/{s3_bucket_name}") \
        .getOrCreate())

    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    glue_context = GlueContext(sc)
    return glue_context



@pytest.fixture(scope="session")
def s3_tables_iceberg_context():
    spark = SparkSession.builder.appName("S3TablesSparkSession") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160",
        ) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "s3tablesbucket") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", f"arn:aws:s3tables:{region}:{account_id}:bucket/{s3_bucket_name}") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "20") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "300000") \
    .config("spark.hadoop.fs.s3a.retry.limit", "15") \
    .config("spark.hadoop.fs.s3a.retry.interval", "2000") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    glue_context = GlueContext(sc)
    return glue_context


#.config("spark.jars", "/Users/marcos.foglino/miscellaneous/neogenomics/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar")
