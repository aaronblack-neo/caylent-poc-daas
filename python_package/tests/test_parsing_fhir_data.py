from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, StringType

from python_package.etl.etl_helper import parse_fhir_condition

from etl.etl_helper import read_fhir_data, parse_fhir_practitioner, parse_fhir_medication


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

from pyspark.sql.functions import col, when, lit

def safe_get(array_col, field, index=0):
    """Safely get a field from an array of structs"""
    return when(
        col(array_col).isNotNull() & (col(array_col).getItem(index).isNotNull()),
        col(array_col).getItem(index).getField(field)
    ).otherwise(lit(None))

def get_struct_field(array_col, field, index=0):
    return when(
        col(array_col).isNotNull() & (col(array_col).getItem(index).isNotNull()),
        col(array_col).getItem(index).getField(field)
    ).otherwise(lit(None))

def get_ingredient_coding_field(df, field):
    return when(
        col("ingredient").isNotNull() &
        col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull() &
        col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").isNotNull() &
        col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").getItem(0).isNotNull(),
        col("ingredient").getItem(0)
        .getField("itemCodeableConcept")
        .getField("coding")
        .getItem(0)
        .getField(field)
    ).otherwise(lit(None))

def test_parsing_fhir_medication(s3_tables_context):
    def get_coding(code_obj):
        if code_obj and 'coding' in code_obj:
            print(f"Extracting coding from: {code_obj}")
            print(f"Extracting coding from: {type(code_obj)}")
            #return code_obj['coding']
            return "hello"
        return None

    # Register UDF
    extract_coding = udf(get_coding)

    #s3_medication_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Medication/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    s3_medication_path_local = "tests/medication/"

    #local_input_path = "/home/hadoop/workspace/00025d1e-2042-4ac9-8e3a-8510629b0564.json"
    #s3_patient_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Patient"
    #s3_condition_path = "s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Condition/000bfd2a-e951-5cec-b412-15a38c05dffe.json"

    # Get Spark session from Glue context
    spark = s3_tables_context.spark_session

    # Read JSON files
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_medication_path_local))

    # Show schema and sample data
    df.printSchema()

    @udf(returnType=ArrayType(StringType()))
    def extract_ingredient_codes(ingredients_array):
        if not ingredients_array:
            return []

        codes = []
        for ingredient in ingredients_array:
            if ingredient and "itemCodeableConcept" in ingredient:
                item_concept = ingredient["itemCodeableConcept"]

                if "coding" in item_concept and item_concept["coding"]:
                    for coding in item_concept["coding"]:
                        if "code" in coding:
                            codes.append(coding["code"])

        return codes

    # select fields id, code
    df_flat = df.select(
        "id",
        col("code.text").alias("code_text"),
        col("ingredient.itemCodeableConcept.text").alias("ingredient_text"),
    )


    df_flat.show(10, truncate=True)
    #df.select("id", col("code.text").alias("code_text"),  col("ingredients.itemCodeableConcept.text").alias("ingredient_text")).show(1000, truncate=False)


    # print("Schema:")
    #df.printSchema()

    #print("\nSample Data:")
    #df.show(1, truncate=False)


def test_parsing_fhir_condition(s3_tables_context):

    s3_condition_path_local = "tests/condition/"

    # Get Spark session from Glue context
    spark = s3_tables_context.spark_session
    # Read JSON files
    df = read_fhir_data(s3_condition_path_local, spark)
    # Show schema and sample data
    df.printSchema()
    # select fields id, code
    #df = df.select("id", "code", explode(col("code")).alias("code_exploded"))
    df = parse_fhir_condition(df)
    df.show(10, truncate=False)


def test_parsing_fhir_observation(glue_context):
    spark = glue_context.spark_session

    table_name = "observation"
    # read iceberg table from raw
    df = spark.sql(f"SELECT * FROM raw.{table_name}")

    df = df.filter(col("id") == "02aa1d78-ac0c-43cd-bd3d-ac632800abc6")
    df.show(10, truncate=True)
    df.printSchema()

    df = df.select("id",
                col("category.text").alias("category_text"),
                col("code.text").alias("code_text"),
                col("effectiveDateTime").alias("effectiveDateTime"),
                col("encounter.reference").alias("encounter_reference"),
                ## col("extension.valueString").alias("extension_value_string"),
                col("identifier.system").alias("identifier_system"),
                col("identifier.value").alias("identifier_value"),
                col("text.status").alias("text_status"),
                #col("interpretation.coding").alias("interpretation_coding"),
                ## col("code.coding.extension.url").alias("code_coding_extension_value_string"),
                col("interpretation.coding").alias("interpretation_coding"),
                col("subject.reference").alias("subject_reference"),
                col("encounter.reference").alias("encounter_reference"),
                col("valueQuantity.code").alias("valueQuantity_code"),
                col("valueQuantity.system").alias("valueQuantity_system"),
                col("valueQuantity.unit").alias("valueQuantity_unit"),
                col("valueQuantity.value").alias("valueQuantity_value"),
                col("valueString").alias("valueString"))


    df.show(10, truncate=False)
    df.printSchema()

def test_parsing_fhir_procedure(glue_context):
    spark = glue_context.spark_session

    table_name = "procedure"
    # read iceberg table from raw
    df = spark.sql(f"SELECT * FROM raw.{table_name}")


    df.show(10, truncate=True)
    df.printSchema()

    df = df.select("id",
                   col("code.text").alias("code_text"),
                   col("encounter.reference").alias("encounter_reference"),
                   col("category.coding.code").alias("category_coding_code"),
                   col("category.coding.display").alias("category_coding_display"),
                   col("identifier.value").alias("identifier_value"),
                   col("subject.reference").alias("subject_reference"),
                   col("text.status").alias("text_status")
                   )

    df.show(10, truncate=False)
    df.printSchema()

def test_parsing_fhir_practitioner(glue_context):
    spark = glue_context.spark_session

    table_name = "practitioner"
    # read iceberg table from raw
    df = spark.sql(f"SELECT * FROM raw.{table_name}")


    df.show(10, truncate=True)
    df.printSchema()

    df = parse_fhir_practitioner(df)

    df.show(10, truncate=False)
    df.printSchema()

def test_parsing_fhir_encounter(glue_context):
    spark = glue_context.spark_session

    table_name = "encounter"
    # read iceberg table from raw
    df = spark.sql(f"SELECT * FROM raw.{table_name}")


    df.show(10, truncate=True)
    df.printSchema()

    df = df.select("id",
                   col("address").alias("address"),
                   col("birthDate").alias("birthDate"),
                   col("gender").alias("gender"),
                   col("identifier").alias("identifier"),
                   col("name").alias("name"),
                   col("telecom").alias("telecom")
                   )

    df.show(10, truncate=False)
    df.printSchema()