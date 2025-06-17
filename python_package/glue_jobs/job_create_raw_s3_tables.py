import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext



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


######################################################
landing_bucket_name = "neogenomics-caylent-shared-data-daas"
def read_case_data(glue_context):
    input_csv_path = f"s3://{landing_bucket_name}/organized_by_table/case_data/CASE_DATA_20250609_0_0_0.csv"
    spark = glue_context.spark_session
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", "|")
        .option("quote", '"')
        .option("multiline", "true")
        .load(input_csv_path)
    )
    return df
###############################################################



case_df = read_case_data(glueContext)
case_df.createOrReplaceTempView("case_data_temp_view")

namespace = "caylent_poc_table_bucket_namespace"
s3_table = "case_data_s3_table"





spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {namespace}.{s3_table} (
              case_hub_id string,
              service_level_name string,
              case_current_workflow_step string,
              case_type_name string,
              designator_code string,
              technology_name string,
              case_body_site_names string,
              case_body_site_names_standard string,
              case_specimen_type_names string,
              case_specimen_type_categories string,
              case_specimen_transport_names string,
              case_panel_codes string,
              case_panel_names string,
              case_overall_result string,
              case_interpretation string,
              case_test_names string
              )
    """)

#spark.sql(f"""SELECT * FROM case_data_temp_view""").show()

spark.sql(f"""
        INSERT INTO {namespace}.{s3_table}
        SELECT * FROM case_data_temp_view
    """)

job.commit()
