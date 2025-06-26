import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import SparkSession

from etl.etl_manager import EtlManager

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "landing_bucket_name",
        "raw_namespace"
    ],
)

MY_TABLE_BUCKET_ARN="arn:aws:s3tables:us-east-1:664418979226:bucket/caylent-poc-table-bucket"

spark = SparkSession.builder.appName("glue-s3-tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3tablesbucket") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", MY_TABLE_BUCKET_ARN) \
    .getOrCreate()

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)
# logger = glueContext.get_logger()

landing_bucket_name = args["landing_bucket_name"]
namespace = args["raw_namespace"]

#tables = list(raw_s3_tables_schemas.keys())
tables = ["patient_match_hub"]

for table in tables:
    #logger.info(f"Processing table: {table}")
    etl_manager = EtlManager(
        spark,
        landing_bucket_name=landing_bucket_name,
        datalake_bucket_name=None,
    )
    etl_manager.process_landing_to_s3_table(table=table, namespace=namespace, delimiter=",")

#job.commit()
