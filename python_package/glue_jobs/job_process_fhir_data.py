import sys
import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from pyspark.sql.types import Row
import time

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
##################################


base_s3_input_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share"

folders = ["Medication", "Patient", "Condition", "Observation", "Practitioner", "Encounter", "Procedure", "Location", "PractitionerRole"]


for folder in folders:
    s3_input_path = f"s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/{folder}"
    logger.info(f"Processing folder: {folder}")

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_input_path))

    table_name = folder.lower()
    df.writeTo(f"raw.{folder}") \
        .tableProperty("format-version", "2") \
        .createOrReplace()


##################################
job.commit()
