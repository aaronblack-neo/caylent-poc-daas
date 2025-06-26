import boto3
from pyspark.sql.functions import regexp_extract, input_file_name
from pyspark.sql.functions import to_date

from etl.config import raw_s3_tables_schemas

TIMESTAMP_COLUMN_NAME = "timestamp"
prefix = "organized_by_table"


class EtlManager:
    def __init__(self, glue_context, landing_bucket_name, datalake_bucket_name):
        #self.glue_client = boto3.client("glue", region_name="us-east-1")
        self.glue_context = glue_context
        self.spark = glue_context
        self.logger = glue_context.get_logger()
        self.landing_bucket_name = landing_bucket_name
        self.datalake_bucket_name = datalake_bucket_name

    def process_landing_data(self, table, delimiter="|"):
        # S3 bucket and prefix

        s3_input_path = f"s3://{self.landing_bucket_name}/{prefix}/{table}"
        target_database = "raw"

        # Read CSV files into a Spark DataFrame,
        # Use | separator
        # adds inferSchema option to automatically infer data types
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", delimiter)
            .option("quote", '"')
            .option("multiline", "true")
            .load(s3_input_path)
        )

        # Extract timestamp from file names
        timestamp_pattern = rf"{table.upper()}_(\d{{8}})"
        df = df.withColumn(TIMESTAMP_COLUMN_NAME, regexp_extract(input_file_name(), timestamp_pattern, 1))

        # timestamp column to timestamp type
        df = df.withColumn(TIMESTAMP_COLUMN_NAME, to_date(df[TIMESTAMP_COLUMN_NAME], "yyyyMMdd"))

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


    def process_landing_to_s3_table(self, table, namespace, delimiter="|"):
        # S3 bucket and prefix

        s3_input_path = f"s3://{self.landing_bucket_name}/{prefix}/{table}"

        # Read CSV files into a Spark DataFrame,
        # Use | separator
        # adds inferSchema option to automatically infer data types
        self.logger.info(f"Reading data from: {s3_input_path}")
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", delimiter)
            .option("quote", '"')
            .option("multiline", "true")
            .load(s3_input_path)
        )
        df.createOrReplaceTempView("data_temp_view")

        self.spark.sql(raw_s3_tables_schemas[table])

        self.logger.info(f"Writing to {namespace}.{table}")
        self.spark.sql(f"""
            INSERT INTO {namespace}.{table}
            SELECT * FROM data_temp_view
        """)

        return df