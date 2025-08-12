import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from etl.etl_helper import write_to_table, parse_fhir_medication_all_exploded, parse_fhir_medicationstatement

# Define the arguments we want to be able to pass to the job
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "database_name"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
##################################

database_name = args["database_name"]

s3_fhir_base_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share"


tables = ["medication"
          # ,"medicationstatement"
 ]


for table_name in tables:
    df = spark.sql(f"SELECT * FROM raw.{table_name}")

    # make a switch case for each folder
    match table_name:
        case "medication":
            df = parse_fhir_medication_all_exploded(df)
        # case "condition":
        #     df = parse_fhir_condition(df)
        # case "observation":
        #     df = parse_fhir_observation(df)
        # case "procedure":
        #     df = parse_fhir_procedure(df)
        # case "patient":
        #     df = parse_fhir_patient(df)
        # case "practitioner":
        #     df = parse_fhir_practitioner(df)
        # case "encounter":
        #     df = parse_fhir_encounter(df)
        case "medicationstatement":
            df = parse_fhir_medicationstatement(df)
        case _:
            logger.error(f"Unknown table: {table_name}. Skipping to next folder.")
            continue

    # Write to Glue Catalog table
    write_to_table(df, database_name, table_name)


job.commit()
