from python_package.tests.test_amazon_comprehend_medical import read_case_data


def test_reading_s3_tables(s3_tables_context):
        spark = s3_tables_context.spark_session
        spark.sql("show schemas in spark_catalog").show()
        spark.sql("show databases").show()




def test_writing_s3_tables(s3_tables_context):
    spark = s3_tables_context.spark_session

    case_df = read_case_data(s3_tables_context)
    case_df.show()
    # catalog = "spark_catalog"
    namespace = "caylent_poc_table_bucket_namespace"
    s3_table = "case_data_s3_table"

    case_df.writeTo(f"{namespace}.{s3_table}") \
        .tableProperty("format-version", "2") \
        .create()


    # if self.table_exists_in_glue_catalog(target_database, table):
    #     df.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").overwritePartitions()
    # else:
    #     df.writeTo(f"{target_database}.{table}").tableProperty("format-version", "2").create()