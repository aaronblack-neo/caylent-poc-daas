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



def parse_fhir_medication(df):
    df = df.select("id", "code", explode(col("ingredient")).alias("ingredients"))
    df = df.select("id", col("code.text").alias("code_text"),
                   col("ingredients.itemCodeableConcept.text").alias("ingredient_text"))
    return df

def write_to_table(df, namespace, table_name):
    df.writeTo(f"{namespace}.{table_name}") \
        .tableProperty("format-version", "2") \
        .createOrReplace()