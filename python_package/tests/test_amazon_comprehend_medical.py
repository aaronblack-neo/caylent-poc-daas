import time

import boto3
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, Row

from python_package.etl.config import landing_bucket_name

comprehend_medical_client = boto3.client("comprehendmedical", region_name="us-east-1")


def test_case_data_with_comprehend_medical(glue_context):

    case_df = read_case_data(glue_context)
    # select field case_interpretation where case_hub_id = 11335065
    case_df.show(100, truncate=False)
    case_df = case_df.filter(case_df.CASE_HUB_ID == "11184653")

    # get all data for that row in a text field
    case_data_sample = case_df.select("CASE_INTERPRETATION").collect()[0]["CASE_INTERPRETATION"]

    print("Sample case data for entity detection:", case_data_sample)

    # Call Amazon Comprehend Medical to detect entities
    response = comprehend_medical_client.detect_entities_v2(Text=case_data_sample)
    print("Response from Comprehend Medical:", response)

    entities = response["Entities"]
    print("Detected entities:", entities["Text"])

    # # Validate the response
    # assert "Entities" in response

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
    input_csv_path = f"s3://{landing_bucket_name}/organized_by_table/case_data/CASE_DATA_20250609_0_0_0.csv"
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


def test_comprehend_for_a_paticular_order():
    # Test for
    # 6311256
    # 11132310
    # 101194376
    case_data_sample = """6347959,11192339,101802889,,,,,1226X,H&E,,468363,0,4760194,1349,ORDERED,FISH,Histology,IHC,Test,,,RESULTED,,,,,,,,,,,,,,2024-09-30 15:48:03.273,2024-10-03 14:19:10.963,true,2025-06-09,6347959,,BCELL^DLBCL^HEME^NHL,,SOLID TUMOR,,New Diagnosis,71,2025-06-09,11192339,Global,Complete,FISH Global,FSG,FISH,Retroperitoneal Mass,RETROPERITONEUM,Paraffin Tissue,Paraffin Tissue,Unstained Slide(s),IHGL,High-Grade/Large B-Cell Lymphoma FISH,Abnormal,BCL2 rearrangements are observed in double/triple-hit lymphomas, 90% of follicular lymphomas, 30% of diffuse large cell lymphomas (sometimes with prior follicular type), and rarely in other lymphoproliferative disorders.
        The >2G signal pattern with the MYC/IGH probe set is suggestive of gain of the IGH locus, trisomy 14 or an IGH rearrangement with a gene other than MYC.
        There is no evidence of MYC or BCL6 rearrangement in this case. Therefore, while this study is ABNORMAL, there is NO EVIDENCE OF A DOUBLE/TRIPLE-HIT LYMPHOMA”.,,2025-06-09,4760194,F,1953-02-09,72,IL,60638,2025-06-09,1349,7247,Loyola University Medical Center Anatomic Patholog,Illinois,IL,60153,United States,US,Academic Medical Center,,Academic Setting,2025-06-09,468363,1326507666,Allopathic & Osteopathic Physicians,Pathology,Anatomic Pathology & Clinical Pathology,PATHOLOGY,IL,606112908,UNITED STATES,US,2025-06-09,0,,,,,OTHER,,,,,2025-06-09,6347959,11192339,101802889,1226X,H&E,101939388,\av1-isilon.neogen.local\clinical\ihc\aperio\images\2024\oct\01\101939388.svs,\av1-isilon.neogen.local|clinical\ihc\aperio\images\2024\oct\01\,101939388.svs,359219521,2024-10-01 02:35:27.447,2024-10-01 09:35:27.447,AV,0,0,First,Image Ready,ss12117,pass,Automatic Instrument QC,Image Ready,2,,,AperioGT450,Aperio Leica Biosystems GT450 v1.0.1,40.00,47963,32456,0.2630000000,,2025-06-09,1349,7247,Loyola University Medical Center Anatomic  
    """
    # print("Sample case data for entity detection:", case_data_sample)

    response = comprehend_medical_client.detect_entities_v2(Text=case_data_sample)
    print("Response from Comprehend Medical:", response)


# def detect_entities_udf(text, client):
#     if text:
#         response = client.detect_entities_v2(Text=text)
#         entities = [entity["Text"] for entity in response.get("Entities", [])]
#         return entities
#     return []
#
# # Register the UDF
# def register_udf(client):
#     return udf(lambda text: detect_entities_udf(text, client), ArrayType(StringType()))
#
# detect_entities = register_udf(comprehend_medical_client)

def test_comprehend_medical_over_case_data_sample_table(glue_context):
    case_df = read_case_data(glue_context)
    # 11507730, 11690041:
    # filter those case_hub_ids
    print("Count:", case_df.count())
    #case_df = case_df.filter(case_df.CASE_HUB_ID.isin(["507730", "11690041"]))

    rows = case_df.collect()

    # Process each row and extract detected entities
    processed_rows = []
    for row in rows:
        case_hub_id = row["CASE_HUB_ID"]
        case_interpretation = row["CASE_INTERPRETATION"]

        # Skip rows with invalid case_interpretation
        if not case_interpretation:
            continue


        response = comprehend_medical_client.detect_entities_v2(Text=case_interpretation)
        detected_entities = [entity["Text"] for entity in response.get("Entities", [])]
        print(f"Detected entities for case_hub_id {case_hub_id}: {detected_entities}")
        processed_rows.append(Row(case_hub_id=case_hub_id, case_interpretation=case_interpretation, detected_entities=detected_entities))

        time.sleep(0.01)

    # Create a new DataFrame with the processed rows
    spark = glue_context.spark_session
    result_df = spark.createDataFrame(processed_rows)

    # Show the resulting DataFrame
    result_df.show(truncate=False)
