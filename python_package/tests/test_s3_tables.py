from python_package.tests.test_amazon_comprehend_medical import read_case_data


def test_reading_s3_tables(s3_tables_context):
        spark = s3_tables_context.spark_session
        spark.sql("show schemas in spark_catalog").show()
        spark.sql("show databases").show()




def test_writing_s3_tables(s3_tables_context):
    spark = s3_tables_context.spark_session

    namespace = "caylent_poc_table_bucket_namespace"
    s3_table = "case_data_s3_table"

    spark.sql("SHOW DATABASES").show()

    spark.sql(f"DESCRIBE NAMESPACE {namespace}").show()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {namespace}.{s3_table} (
           id INT,
           name STRING,
           value INT
        )
    """)

    spark.sql(f"""
        INSERT INTO {namespace}.{s3_table}
        VALUES
           (1, 'ABC', 100),
           (2, 'XYZ', 200)
    """)

    spark.sql(f"SELECT * FROM {namespace}.{s3_table} LIMIT 10").show()


def test_writing_case_data(s3_tables_context):
    spark = s3_tables_context.spark_session

    case_df = read_case_data(s3_tables_context)
    case_df.show()

    namespace = "caylent_poc_table_bucket_namespace"
    s3_table = "case_data_s3_table"

    case_df.writeTo(f"{namespace}.{s3_table}") \
        .tableProperty("format-version", "2") \
        .create()