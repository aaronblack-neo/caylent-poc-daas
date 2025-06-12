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
    input_csv_path = "s3://caylent-poc-dl-landing/case_data/CASE_DATA_20250609_0_0_0.csv"
    spark = glue_context.spark_session

    df = (spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .option("quote", '"') \
        .option("multiline", "true") \
        .load(input_csv_path))
    df.show()
