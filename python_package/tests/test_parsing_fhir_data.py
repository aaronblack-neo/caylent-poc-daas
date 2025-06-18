

def test_parsing_fhir_data(glue_context):
    s3_input_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication"
    local_input_path = "/home/hadoop/workspace/00025d1e-2042-4ac9-8e3a-8510629b0564.json"

    # Get Spark session from Glue context
    spark = glue_context.spark_session

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          #.option("inferSchema", "true")
          .json(local_input_path))

    # Show schema and sample data
    print("Schema:")
    df.printSchema()

    print("\nSample Data:")
    df.show(5, truncate=False)

    # Get total count
    print(f"\nTotal records: {df.count()}")