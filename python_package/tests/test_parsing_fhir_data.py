from pyspark.sql.functions import udf, col, explode


def test_writing_fhir_data(s3_tables_context):
    s3_medication_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #local_input_path = "/home/hadoop/workspace/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #s3_patient_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Patient"
    #s3_condition_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Condition/000bfd2a-e951-5cec-b412-15a38c05dffe.json"

    # Get Spark session from Glue context
    spark = s3_tables_context.spark_session

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_medication_path))

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

    table = "condition_test_23"
    (df.writeTo(f"raw.{table}")
     .tableProperty("format-version", "2")
     .tableProperty("catalog-name", "glue_catalog")
     #.tableProperty("warehouse", f"s3://{self.datalake_bucket_name}/tables")
     .createOrReplace())

    # Force metadata synchronization
    spark.sql(f"REFRESH TABLE raw.{table}")


def test_reading_fhir_data(s3_tables_context):
    spark = s3_tables_context.spark_session

    #spark.sql("SELECT * FROM s3tables.raw_local.medication_local").show()
    #spark.sql("SELECT * FROM raw.medication_test").show(1, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.medication_test").show(10, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.doctor_data").show(10, truncate=False)
    #spark.sql("SHOW CREATE TABLE raw.case_data").show(10, truncate=False)

    # spark.sql("DROP TABLE PURGE raw_local.medication_local").show(10, truncate=False)
    #spark.sql("DROP TABLE PURGE raw.medication_test").show(10, truncate=False)
    #spark.sql("DROP TABLE PURGE raw.medication").show(10, truncate=False)

    #spark.sql("DROP NAMESPACE raw_local").show(10, truncate=False)
    #spark.sql("SHOW NAMESPACES").show(10, truncate=False)
    #spark.sql("SELECT * FROM raw.medication").show(10, truncate=True)
    spark.sql("SHOW CREATE TABLE raw.medication").show(10, truncate=False)


@udf
def extract_ingredient(ingredients):
    if ingredients and len(ingredients)>0:
        print(f"Extracting ingredients from: {ingredients[0]}")
        #return code_obj['coding'][0]
        return "ingredient"
    return None



def test_parsing_fhir_data(s3_tables_context):
    def get_coding(code_obj):
        if code_obj and 'coding' in code_obj:
            print(f"Extracting coding from: {code_obj}")
            print(f"Extracting coding from: {type(code_obj)}")
            #return code_obj['coding']
            return "hello"
        return None

    # Register UDF
    extract_coding = udf(get_coding)

    s3_medication_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #local_input_path = "/home/hadoop/workspace/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #s3_patient_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Patient"
    #s3_condition_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Condition/000bfd2a-e951-5cec-b412-15a38c05dffe.json"

    # Get Spark session from Glue context
    spark = s3_tables_context.spark_session

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_medication_path))

    # Show schema and sample data

    # select fields id, code
    df = df.select("id", "code", extract_coding(col("code")).alias("coding"), explode(col("code")).alias("exploded_code"))
    df.select("id", "exploded_code.coding").show(1, truncate=False)


    # print("Schema:")
    df.printSchema()

    #print("\nSample Data:")
    #df.show(1, truncate=False)