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


def test_writing_s3_tables_2(s3_tables_iceberg_context):
    spark = s3_tables_iceberg_context.spark_session

    namespace = "caylent_poc_table_bucket_namespace"
    s3_table = "simple_s3_table_2"

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
        SELECT * FROM case_data_temp_view where case_hub_id in  ('11828538','11597031','11767722')
    """)


