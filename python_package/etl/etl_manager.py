from etl.etl_helper import write_to_table

prefix = "organized_by_table"


class EtlManager:
    def __init__(self, glue_context, landing_bucket_name, datalake_bucket_name):
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
            #.option("multiline", "true")
            .load(s3_input_path)
        )

        write_to_table(df, "raw", table)

        return df
