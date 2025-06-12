from pyspark.sql.functions import input_file_name, regexp_extract


def test_loading_data(glue_context):
    from etl.etl_manager import EtlManager

    landing_bucket_name = "caylent-poc-dl-landing"
    raw_bucket_name = "caylent-poc-dl-raw"

    etl_manager = EtlManager(glue_context, landing_bucket_name, raw_bucket_name)

    # Test processing a specific table
    #table = "accession_data"
    table = "case_data"
    df = etl_manager.process_landing_data(table)
    df.show()


def test_reading_case_data(glue_context):
    input_csv_path = "s3://caylent-poc-dl-landing/case_data/"
    spark = glue_context.spark_session

    # case_hub_id

    df = (spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .option("quote", '"') \
        .option("multiline", "true") \
        .load(input_csv_path))

    # select case_hub_id and cast case_hub_id to int
    from pyspark.sql.functions import col
    df2 = df.select(col("case_hub_id").cast("int").alias("case_hub_id"))
    df2.show(100, truncate=False)

    # sum all values in column case_hub_id
    df = df2.groupBy().sum("case_hub_id").collect()[0][0]
    print(f"Sum of case_hub_id: {df}")

def test_reading_accession_data(glue_context):
    input_csv_path = "s3://caylent-poc-dl-landing/accession_data"
    spark = glue_context.spark_session
    table= "accession_data"

    # case_hub_id
    TIMESTAMP_COLUMN_NAME = "timecolumn"
    df = (spark.read.format("csv") \
          .option("header", "true") \
          .option("delimiter", "|") \
          .option("quote", '"') \
          .option("multiline", "true") \
          .option("inferSchema", "true") \
          # .option("dateFormat", "yyyyMMdd") \
          # .option("timestampFormat", "yyyyMMdd") \
          .load(input_csv_path))

    timestamp_pattern = rf"{table.upper()}_(\d{{8}})"
    df = df.withColumn(TIMESTAMP_COLUMN_NAME, regexp_extract(input_file_name(), timestamp_pattern, 1))
    df = df.withColumn(TIMESTAMP_COLUMN_NAME, regexp_extract(TIMESTAMP_COLUMN_NAME, r"(\d{4})(\d{2})(\d{2})", 1).cast("timestamp"))

    df.show(100, truncate=False)

    # show schema
    df.printSchema()


