import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.config import raw_s3_tables_schemas
from etl.etl_manager import EtlManager

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "landing_bucket_name",
        "datalake_bucket_name",
        "table_name",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Retrieve parameters from the job arguments

landing_bucket_name = args["landing_bucket_name"]
datalake_bucket_name = args["datalake_bucket_name"]
table_name = args["table_name"]

tables = ["pati"]

for table_name in tables:
    etl_manager = EtlManager(
        glueContext,
        landing_bucket_name=landing_bucket_name,
        datalake_bucket_name=datalake_bucket_name,
    )
    latest_data_df = etl_manager.process_landing_data(table=table_name, delimiter=raw_s3_tables_schemas[table_name]["delimiter"])

job.commit()
