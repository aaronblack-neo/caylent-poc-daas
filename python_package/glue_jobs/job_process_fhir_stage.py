import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext


from etl.etl_helper import write_to_table, parse_fhir_medication, parse_fhir_condition

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

s3_fhir_base_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share"
folders = [ "Condition"]

for folder in folders:
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(f"{s3_fhir_base_path}/{folders[0]}"))


    # make a switch case for each folder
    match folder:
        case "Medication":
            df = parse_fhir_medication(df)
        case "Condition":
            df = parse_fhir_condition(df)
        case _:
            logger.error(f"Unknown folder: {folder}. Skipping to next folder.")
            continue

    # Write to Glue Catalog table
    table_name = folder.lower()
    write_to_table(df, namespace, table_name)




job.commit()
