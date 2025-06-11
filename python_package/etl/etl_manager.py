import boto3

from pyspark.sql.functions import regexp_extract, lit
from pyspark.sql.functions import regexp_extract, input_file_name


TIMESTAMP_COLUMN_NAME = "timestamp"


class EtlManager:
    def __init__(self, glue_context, landing_bucket_name, raw_bucket_name):
        self.glue_client = boto3.client("glue", region_name="us-east-1")
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.logger = glue_context.get_logger()
        self.landing_bucket_name = landing_bucket_name
        self.raw_bucket_name = raw_bucket_name

    def process_landing_data(self, table):
        # S3 bucket and prefix

        s3_input_path = f"s3://{self.landing_bucket_name}/{table}"
        iceberg_table_path = f"s3://{self.raw_bucket_name}/tables/{table}/"
        target_database = "raw"

        # Read CSV files into a Spark DataFrame,
        # Use | separator
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "|")
            .load(s3_input_path)
        )

        # Extract timestamp from file names
        timestamp_pattern = rf"{table.upper()}_(\d{{8}})"
        df = df.withColumn(TIMESTAMP_COLUMN_NAME, regexp_extract(input_file_name(), timestamp_pattern, 1))

        if self.table_exists_in_glue_catalog(target_database, table):
            df.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").overwritePartitions()
        else:
            df.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").create()

        return df

    def table_exists_in_glue_catalog(self, database_name, table_name):
        try:
            result = self.spark.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'").count()
            return result > 0
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False
