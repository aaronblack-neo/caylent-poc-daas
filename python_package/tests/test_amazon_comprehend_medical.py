import boto3
import pytest


comprehend_medical_client = boto3.client("comprehendmedical", region_name="us-east-1")

def test_case_data_with_comprehend_medical(glue_context):
    # Mock input data from the case_data table
    case_data_sample = "Patient has a history of diabetes and hypertension."

    case_df = read_case_data(glue_context)
    # select field case_interpretation where case_hub_id = 11335065
    case_df.show(100, truncate=False)
    case_df = case_df.filter(case_df.CASE_HUB_ID == '11184653')

    # get all data for that row in a text field
    case_data_sample = case_df.select("CASE_INTERPRETATION").collect()[0]["CASE_INTERPRETATION"]

    print("Sample case data for entity detection:", case_data_sample)

    # Call Amazon Comprehend Medical to detect entities
    response = comprehend_medical_client.detect_entities_v2(Text=case_data_sample)
    print("Response from Comprehend Medical:", response)

    # # Validate the response
    # assert "Entities" in response
    # entities = response["Entities"]
    #
    # # Check for specific medical conditions
    # conditions = [entity["Text"] for entity in entities if entity["Category"] == "MEDICAL_CONDITION"]
    # assert "diabetes" in conditions
    # assert "hypertension" in conditions
    #
    # # Print the detected entities (optional)
    # print("Detected medical conditions:", conditions)



def test_reading_data(glue_context):
    df = read_case_data(glue_context)

    df.show()


def read_case_data(glue_context):
    input_csv_path = "s3://caylent-poc-dl-landing/case_data/CASE_DATA_20250609_0_0_0.csv"
    spark = glue_context.spark_session
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", "|")
        .option("quote", '"')
        .option("multiline", "true")
        .load(input_csv_path)
    )
    return df
