import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.config import landing_bucket_name
from etl.etl_manager import EtlManager

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

landing_bucket_name = "neogenomics-caylent-shared-data-daas"
namespace = "caylent_poc_table_bucket_namespace"

tables = ["accession_data"]

for table in tables:
    etl_manager = EtlManager(
        glueContext,
        landing_bucket_name=landing_bucket_name,
        datalake_bucket_name=None,
    )
    etl_manager.process_landing_to_s3_table(table=table, namespace=namespace)

job.commit()
