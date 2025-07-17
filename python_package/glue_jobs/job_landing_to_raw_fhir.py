import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.etl_helper import write_to_table

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "database_name"
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
##################################

database_name = args['database_name']
base_s3_input_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share"

# prescribed folders
folders = [
    "Medication",
    "Patient",
    "Condition",
    "Observation",
    "Practitioner",
    "Encounter",
    "Procedure",
    "Location",
    "PractitionerRole"
    #, "MedicationStatement"
]

for folder in folders:
    s3_input_path = f"s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/{folder}"
    logger.info(f"Processing folder: {folder}")

    try:
        # Read JSON files
        df = spark.read.option("multiline", "true").option("inferSchema", "true").json(s3_input_path)

        table_name = folder.lower()

        write_to_table(df, database_name, table_name)

    except:
        logger.error(f"Error processing folder {folder}. Skipping to next folder.")

##################################
job.commit()
