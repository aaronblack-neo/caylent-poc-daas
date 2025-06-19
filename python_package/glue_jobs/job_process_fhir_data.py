import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from pyspark.sql.types import Row
import time

from python_package.etl.config import namespace

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


s3_input_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication"
namespace = "raw"




# Read JSON files
df = (spark.read
      .option("multiline", "true")
      #.option("inferSchema", "true")
      .json(s3_input_path))

# Show schema and sample data
logger.info(f"Schema: {df.printSchema()}")



df.show(5, truncate=False)

# Get total count
logger.info(f"Total records: {df.count()}")

##################################
job.commit()
