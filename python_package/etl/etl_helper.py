from pyspark.sql.functions import col, explode


def read_fhir_data(s3_condition_path_local, spark):
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(s3_condition_path_local))
    return df


def parse_fhir_condition(df):
    df = df.select("id", col("code.text").alias("code_text"),
                   col("clinicalStatus.text").alias("clinical_status_text"),
                   col("meta.source").alias("meta_source"),
                   col("subject.reference").alias("subject_reference"))
    return df

def parse_fhir_observation(df):
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
                   col("valueQuantity.code").alias("valueQuantity_code"),
                   col("valueQuantity.system").alias("valueQuantity_system"),
                   col("valueQuantity.unit").alias("valueQuantity_unit"),
                   col("valueQuantity.value").alias("valueQuantity_value"),
                   col("valueString").alias("valueString"))
    return df

def parse_fhir_medication(df):
    df = df.select("id", "code", explode(col("ingredient")).alias("ingredients"))
    df = df.select("id", col("code.text").alias("code_text"),
                   col("ingredients.itemCodeableConcept.text").alias("ingredient_text"))
    return df


def parse_fhir_procedure(df):
    df.select("id",
              col("code.text").alias("code_text"),
              col("encounter.reference").alias("encounter_reference"),
              col("category.coding.code").alias("category_coding_code"),
              col("category.coding.display").alias("category_coding_display"),
              col("identifier.value").alias("identifier_value"),
              col("subject.reference").alias("subject_reference"),
              col("text.status").alias("text_status")
              )
    return df


def parse_fhir_patient(df):
    df = df.select("id",
                   col("gender").alias("gender"),
                   )
    return df


def parse_fhir_practitioner(df):
    df = df.select("id",
                   col("address.city").alias("address_city"),
                   col("address.country"),
                   col("address.district"),
                   col("address.line"),
                   col("address.postalCode"),
                   col("address.state"),
                   col("address.use"),
                   #col("identifier.value").alias("identifier_value"),
                   col("name.family").alias("name_family"),
                   col("name.suffix").alias("name_suffix"),
                   col("name.use").alias("name_use"),
                   col("telecom.value").alias("phone_number"),
                   )
    return df

def parse_fhir_encounter(df):
    pass
    #return df

def write_to_table(df, namespace, table_name):
    df.writeTo(f"{namespace}.{table_name}") \
        .tableProperty("format-version", "2") \
        .createOrReplace()