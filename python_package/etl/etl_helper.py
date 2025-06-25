from pyspark.sql.functions import col, explode, lit, when, udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType


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


    df_flat = (df
               .withColumn("code_text", col("code.text"))
               .withColumn("first_coding_system", col("code.coding").getItem(0).getField("system"))
               .withColumn("first_coding_code", col("code.coding").getItem(0).getField("code"))
               .withColumn("first_coding_display", col("code.coding").getItem(0).getField("display"))
               # Extract ingredient information if available
               .withColumn("ingredient_text",
                           when(col("ingredient").isNotNull() &
                                col("ingredient").getItem(0).isNotNull() &
                                col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull(),
                                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("text"))
                           .otherwise(lit(None)))
               .withColumn("ingredient_coding_code",
                           when(col("ingredient").isNotNull() &
                                col("ingredient").getItem(0).isNotNull() &
                                col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull() &
                                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").isNotNull(),
                                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").getItem(0).getField("code"))
                           .otherwise(lit(None)))
               .withColumn("ingredient_coding_display",
                           when(col("ingredient").isNotNull() &
                                col("ingredient").getItem(0).isNotNull() &
                                col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull() &
                                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").isNotNull(),
                                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").getItem(0).getField("display"))
                           .otherwise(lit(None)))
               .withColumn("ingredient_coding_codes", extract_ingredient_codes(col("ingredient")))
               .withColumn("ingredient_texts", col("ingredient.itemCodeableConcept.text"))
               )

    df_flat = df_flat.select(
        "id",
        "code_text",
        "first_coding_system",
        "first_coding_code",
        "first_coding_display",
        "ingredient_text",
        "ingredient_coding_code",
        "ingredient_coding_display",
        "ingredient_coding_codes",
        "ingredient_texts",
    )
    return df_flat


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

"""
root
 |-- class: struct (nullable = true)
 |    |-- display: string (nullable = true)
 |-- diagnosis: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- condition: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |    |    |-- rank: long (nullable = true)
 |-- extension: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- valueString: string (nullable = true)
 |-- hospitalization: struct (nullable = true)
 |    |-- dischargeDisposition: struct (nullable = true)
 |    |    |-- coding: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |-- text: string (nullable = true)
 |-- id: string (nullable = true)
 |-- identifier: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- assigner: struct (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- location: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- location: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |-- meta: struct (nullable = true)
 |    |-- source: string (nullable = true)
 |-- participant: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- individual: struct (nullable = true)
 |    |    |    |-- reference: string (nullable = true)
 |    |    |-- type: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |-- text: string (nullable = true)
 |-- period: struct (nullable = true)
 |    |-- end: string (nullable = true)
 |    |-- start: string (nullable = true)
 |-- resourceType: string (nullable = true)
 |-- status: string (nullable = true)
 |-- subject: struct (nullable = true)
 |    |-- reference: string (nullable = true)
 |-- text: struct (nullable = true)
 |    |-- div: string (nullable = true)
 |    |-- status: string (nullable = true)
 |-- type: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- coding: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |    |-- extension: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |    |    |    |-- valueInteger: long (nullable = true)
 |    |    |    |    |    |    |-- valueString: string (nullable = true)
 |    |    |    |    |-- system: string (nullable = true)
 |    |    |-- text: string (nullable = true)"""

def parse_fhir_encounter(df):
    df = df.select("id",
                   col("class.display"),
                   col("diagnosis.condition.reference").alias("diagnosis_condition_reference"),
                   col("hospitalization.dischargeDisposition.text").alias("discharge_disposition_text"),
                   col("identifier.assigner.display").alias("identifier_assigner_display"),
                   col("identifier.value").alias("identifier_value"),
                   col("location.location.reference").alias("location_location_reference"),
                     col("meta.source").alias("meta_source"),
                     col("participant.individual.reference").alias("participant_individual_reference"),
                     col("period.start").alias("period_start"),
                     col("period.end").alias("period_end"),
                     col("resourceType").alias("resource_type"),
                     col("status").alias("status"),
                     col("subject.reference").alias("subject_reference"),
                     col("text.status").alias("text_status"),
                     col("type.text").alias("type_text"),
                   )
    return df


def parse_fhir_medication_alternative(df):
    # Define UDFs for all normalized concept fields
    @udf(returnType=IntegerType())
    def extract_concept_id(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:id" and "valueInteger" in ext:
                    return ext["valueInteger"]
        return None

    @udf(returnType=StringType())
    def extract_concept_code(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:code" and "valueString" in ext:
                    return ext["valueString"]
        return None

    @udf(returnType=StringType())
    def extract_concept_name(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:name" and "valueString" in ext:
                    return ext["valueString"]
        return None

    @udf(returnType=StringType())
    def extract_concept_vocabulary_id(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:vocabularyId" and "valueString" in ext:
                    return ext["valueString"]
        return None

    @udf(returnType=StringType())
    def extract_concept_standard(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:standard" and "valueString" in ext:
                    return ext["valueString"]
        return None

    @udf(returnType=BooleanType())
    def extract_concept_classification_cancer(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:classification:cancer" and "valueBoolean" in ext:
                    return ext["valueBoolean"]
        return None

    @udf(returnType=StringType())
    def extract_concept_domain(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:domain" and "valueString" in ext:
                    return ext["valueString"]
        return None

    @udf(returnType=StringType())
    def extract_concept_class(extension_array):
        if extension_array:
            for ext in extension_array:
                if "url" in ext and ext["url"] == "urn:omop:normalizedConcept:class" and "valueString" in ext:
                    return ext["valueString"]
        return None

    # Apply all UDFs to extract values
    extension_col = col("code.coding").getItem(0).getField("extension")
    result_df = df.select(
        "id",
        extract_concept_id(extension_col).alias("normalized_concept_id"),
        extract_concept_code(extension_col).alias("normalized_concept_code"),
        extract_concept_name(extension_col).alias("normalized_concept_name"),
        extract_concept_vocabulary_id(extension_col).alias("normalized_concept_vocabulary_id"),
        extract_concept_standard(extension_col).alias("normalized_concept_standard"),
        extract_concept_classification_cancer(extension_col).alias("normalized_concept_classification_cancer"),
        extract_concept_domain(extension_col).alias("normalized_concept_domain"),
        extract_concept_class(extension_col).alias("normalized_concept_class")
    )

    return result_df


def write_to_table(df, namespace, table_name):
    df.writeTo(f"{namespace}.{table_name}") \
        .tableProperty("format-version", "2") \
        .createOrReplace()