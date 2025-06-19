

def test_parsing_fhir_data(s3_tables_context):
    s3_input_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #local_input_path = "/home/hadoop/workspace/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    s3_patient_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Patient"
    s3_condition_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Condition/000bfd2a-e951-5cec-b412-15a38c05dffe.json"

    # Get Spark session from Glue context
    spark = s3_tables_context.spark_session

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_condition_path))

    # Show schema and sample data
    print("Schema:")
    df.printSchema()

    print("\nSample Data:")
    df.show(5, truncate=False)

    # spark.sql("CREATE NAMESPACE IF NOT EXISTS s3tables.raw_local")
    #
    # df.writeTo("raw_local.medication_local") \
    #     .tableProperty("format-version", "2") \
    #     .createOrReplace()

def test_reading_fhir_data(s3_tables_context):
    spark = s3_tables_context.spark_session

    #spark.sql("SELECT * FROM s3tables.raw_local.medication_local").show()
    #spark.sql("SELECT * FROM raw.medication_test").show(1, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.medication_test").show(10, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.doctor_data").show(10, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.case_data").show(10, truncate=False)

    # spark.sql("DROP TABLE PURGE raw_local.medication_local").show(10, truncate=False)
    spark.sql("DROP TABLE PURGE raw.medication_test").show(10, truncate=False)
    spark.sql("DROP TABLE PURGE raw.medication").show(10, truncate=False)

    #spark.sql("DROP NAMESPACE raw_local").show(10, truncate=False)
    #spark.sql("SHOW NAMESPACES").show(10, truncate=False)
    #spark.sql("SELECT count(*) FROM raw.medication").show(10, truncate=False)

