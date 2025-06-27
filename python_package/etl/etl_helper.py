from pyspark.sql.functions import col, explode, lit, when, udf, explode_outer
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType


def read_fhir_data(s3_condition_path_local, spark):
    df = spark.read.option("multiline", "true").option("inferSchema", "true").json(s3_condition_path_local)
    return df

def write_to_table(df, namespace, table_name):
    df.writeTo(f"{namespace}.{table_name}").tableProperty("format-version", "2").createOrReplace()

def parse_fhir_condition(df):
    df = df.select(
        "id",
        col("code.text").alias("code_text"),
        col("clinicalStatus.text").alias("clinical_status_text"),
        col("meta.source").alias("meta_source"),
        col("subject.reference").alias("subject_reference"),
    )
    return df

def parse_fhir_observation(df):
    df = df.select(
        "id",
        col("category.text").alias("category_text"),
        col("code.text").alias("code_text"),
        col("effectiveDateTime").alias("effectiveDateTime"),
        col("encounter.reference").alias("encounter_reference"),
        ## col("extension.valueString").alias("extension_value_string"),
        col("identifier.system").alias("identifier_system"),
        col("identifier.value").alias("identifier_value"),
        col("text.status").alias("text_status"),
        # col("interpretation.coding").alias("interpretation_coding"),
        ## col("code.coding.extension.url").alias("code_coding_extension_value_string"),
        col("interpretation.coding").alias("interpretation_coding"),
        col("subject.reference").alias("subject_reference"),
        col("valueQuantity.code").alias("valueQuantity_code"),
        col("valueQuantity.system").alias("valueQuantity_system"),
        col("valueQuantity.unit").alias("valueQuantity_unit"),
        col("valueQuantity.value").alias("valueQuantity_value"),
        col("valueString").alias("valueString"),
    )
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

    df_flat = (
        df.withColumn("code_text", col("code.text"))
        .withColumn("first_coding_system", col("code.coding").getItem(0).getField("system"))
        .withColumn("first_coding_code", col("code.coding").getItem(0).getField("code"))
        .withColumn("first_coding_display", col("code.coding").getItem(0).getField("display"))
        # Extract ingredient information if available
        .withColumn(
            "ingredient_text",
            when(
                col("ingredient").isNotNull()
                & col("ingredient").getItem(0).isNotNull()
                & col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull(),
                col("ingredient").getItem(0).getField("itemCodeableConcept").getField("text"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "ingredient_coding_code",
            when(
                col("ingredient").isNotNull()
                & col("ingredient").getItem(0).isNotNull()
                & col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull()
                & col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").isNotNull(),
                col("ingredient")
                .getItem(0)
                .getField("itemCodeableConcept")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "ingredient_coding_display",
            when(
                col("ingredient").isNotNull()
                & col("ingredient").getItem(0).isNotNull()
                & col("ingredient").getItem(0).getField("itemCodeableConcept").isNotNull()
                & col("ingredient").getItem(0).getField("itemCodeableConcept").getField("coding").isNotNull(),
                col("ingredient")
                .getItem(0)
                .getField("itemCodeableConcept")
                .getField("coding")
                .getItem(0)
                .getField("display"),
            ).otherwise(lit(None)),
        )
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
    df.select(
        "id",
        col("code.text").alias("code_text"),
        col("encounter.reference").alias("encounter_reference"),
        col("category.coding.code").alias("category_coding_code"),
        col("category.coding.display").alias("category_coding_display"),
        col("identifier.value").alias("identifier_value"),
        col("subject.reference").alias("subject_reference"),
        col("text.status").alias("text_status"),
    )
    return df

def parse_fhir_patient(df):
    df = df.select(
        "id",
        col("gender").alias("gender"),
    )
    return df

def parse_fhir_practitioner(df):
    df = df.select(
        "id",
        col("address.city").alias("address_city"),
        col("address.country"),
        col("address.district"),
        col("address.line"),
        col("address.postalCode"),
        col("address.state"),
        col("address.use"),
        # col("identifier.value").alias("identifier_value"),
        col("name.family").alias("name_family"),
        col("name.suffix").alias("name_suffix"),
        col("name.use").alias("name_use"),
        col("telecom.value").alias("phone_number"),
    )
    return df

def parse_fhir_encounter(df):
    df = df.select(
        "id",
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

def parse_fhir_medication_all_exploded(df):
    # Part 1: Explode the code.coding array
    exploded_coding_df = df.select("id", explode(col("code.coding")).alias("coding_item"))

    # Extract the normalized concept data from each coding
    coding_result_df = exploded_coding_df.select(
        col("id").alias("main_id"),
        lit("code").alias("source_type"),
        col("coding_item.system").alias("system"),
        col("coding_item.code").alias("item_code"),
        col("coding_item.display").alias("display"),
        extract_concept_id(col("coding_item.extension")).alias("id"),
        extract_concept_code(col("coding_item.extension")).alias("code"),
        extract_concept_name(col("coding_item.extension")).alias("name"),
        extract_concept_vocabulary_id(col("coding_item.extension")).alias("vocabulary_id"),
        extract_concept_standard(col("coding_item.extension")).alias("standard"),
        extract_concept_classification_cancer(col("coding_item.extension")).alias("cancer"),
        extract_concept_domain(col("coding_item.extension")).alias("domain"),
        extract_concept_class(col("coding_item.extension")).alias("class"),
    )

    # Part 2: Explode the ingredient array
    exploded_ingredient_df = df.select("id", explode(col("ingredient")).alias("ingredient_item"))

    # Extract the normalized concept data from each ingredient
    extension_col = col("ingredient_item.itemCodeableConcept.coding").getItem(0).getField("extension")
    ingredient_result_df = exploded_ingredient_df.select(
        col("id").alias("main_id"),
        lit("ingredient").alias("source_type"),
        col("ingredient_item.itemCodeableConcept.coding").getItem(0).getField("system").alias("system"),
        col("ingredient_item.itemCodeableConcept.coding").getItem(0).getField("code").alias("item_code"),
        col("ingredient_item.itemCodeableConcept.text").alias("display"),
        extract_concept_id(extension_col).alias("id"),
        extract_concept_code(extension_col).alias("code"),
        extract_concept_name(extension_col).alias("name"),
        extract_concept_vocabulary_id(extension_col).alias("vocabulary_id"),
        extract_concept_standard(extension_col).alias("standard"),
        extract_concept_classification_cancer(extension_col).alias("cancer"),
        extract_concept_domain(extension_col).alias("domain"),
        extract_concept_class(extension_col).alias("class"),
    )

    # Combine both results
    result_df = coding_result_df.union(ingredient_result_df)
    return result_df

def parse_fhir_medicationstatement(df):
    """
    Parse FHIR MedicationStatement resources and extract normalized concepts.

    Args:
        df: DataFrame containing MedicationStatement resources

    Returns:
        DataFrame with exploded concept data from reasonCode.coding
    """
    # First filter to include only records that have reasonCode
    df_with_reasons = df.filter(col("reasonCode").isNotNull())

    # Skip processing if no records with reasonCode
    if df_with_reasons.count() == 0:
        return df_with_reasons.select(
            "id",
            lit("reasonCode").alias("source_type"),
            lit(None).alias("reason_text"),
            lit(None).alias("coding_display"),
            lit(None).alias("concept_id")
        ).limit(0)  # Empty dataframe with expected schema

    # Extract the reasonCode array and explode it
    exploded_reason_df = df_with_reasons.select(
        "id",
        explode_outer(col("reasonCode")).alias("reason_item")
    )

    # Explode the coding array for each reasonCode
    exploded_coding_df = exploded_reason_df.select(
        "id",
        explode_outer(col("reason_item.coding")).alias("coding_item"),
        col("reason_item.text").alias("reason_text")
    )

    # Extract normalized concept data from each coding
    result_df = exploded_coding_df.select(
        col("id"),
        lit("reasonCode").alias("source_type"),
        col("reason_text"),
        col("coding_item.display").alias("coding_display"),
        # Extract concept information from extensions
        extract_concept_id(col("coding_item.extension")).alias("concept_id"),
        extract_concept_code(col("coding_item.extension")).alias("concept_code"),
        extract_concept_name(col("coding_item.extension")).alias("concept_name"),
        extract_concept_vocabulary_id(col("coding_item.extension")).alias("concept_vocabulary_id"),
        extract_concept_standard(col("coding_item.extension")).alias("concept_standard"),
        extract_concept_classification_cancer(col("coding_item.extension")).alias("concept_cancer"),
        extract_concept_domain(col("coding_item.extension")).alias("concept_domain"),
        extract_concept_class(col("coding_item.extension")).alias("concept_class")
    )

    return result_df




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
            if (
                "url" in ext
                and ext["url"] == "urn:omop:normalizedConcept:classification:cancer"
                and "valueBoolean" in ext
            ):
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
