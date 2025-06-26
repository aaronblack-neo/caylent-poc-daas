import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.config import raw_s3_tables_schemas
from etl.etl_helper import write_to_table

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "namespace"
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
##################################

namespace = args["namespace"]

# List of CSV tables to process
csv_tables = raw_s3_tables_schemas.keys()

for table_name in csv_tables:
    logger.info(f"Processing table: {table_name}")

    # Read from raw table
    df = spark.sql(f"SELECT * FROM raw.{table_name}")

    # Write to stage layer
    write_to_table(df, namespace, table_name)
    logger.info(f"Successfully processed {table_name}")

job.commit()