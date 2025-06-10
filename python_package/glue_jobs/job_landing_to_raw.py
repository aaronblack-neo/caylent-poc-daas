import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.bookmark_manager import BookmarkManager
from etl.etl_manager import EtlManager

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "landing_bucket_name",
        "raw_bucket_name",
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
raw_bucket_name = args["raw_bucket_name"]


etl_manager = EtlManager(
    bookmark_manager,
    glueContext,
    landing_bucket_name=landing_bucket_name,
    bucket_prefix=bucket_prefix,
    raw_bucket_name=raw_bucket_name,
)
latest_data_df = etl_manager.process_landing_data(table="people_table", timestamp_bookmark_str=timestamp_bookmark_str)
latest_data_df.show()

job.commit()