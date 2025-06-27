landing_bucket_name = "neogenomics-caylent-shared-data-daas"
landing_bucket_filepath = "neogenomics-caylent-shared-data-daas/organized_by_table"
datalake_bucket_name = "caylent-poc-datalake"
namespace = "raw"


csv_tables = {
    "case_data": {
        "delimiter": "|"
    },
    "accession_data": {
        "delimiter": "|"
    },
    "client_data": {
        "delimiter": "|"
    },
    "doctor_data": {
        "delimiter": "|"
    },
    "image_data": {
        "delimiter": "|"
    },
    "orders_fact_data": {
        "delimiter": "|"
    },
    "patient_data": {
        "delimiter": "|"
    },
    "patient_match_hub": {
        "delimiter": ","
    },
    "new_table": {
        "delimiter": "|"
    }
}
