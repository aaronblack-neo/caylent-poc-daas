
landing_bucket_name = "neogenomics-caylent-shared-data-daas"
landing_bucket_filepath = "neogenomics-caylent-shared-data-daas/organized_by_table"
datalake_bucket_name = "caylent-poc-datalake"

namespace = "caylent_poc_table_bucket_namespace"


raw_s3_tables_schemas = {
    "case_data": f"""
        CREATE TABLE IF NOT EXISTS {namespace}.case_data (
              case_hub_id string,
              service_level_name string,
              case_current_workflow_step string,
              case_type_name string,
              designator_code string,
              technology_name string,
              case_body_site_names string,
              case_body_site_names_standard string,
              case_specimen_type_names string,
              case_specimen_type_categories string,
              case_specimen_transport_names string,
              case_panel_codes string,
              case_panel_names string,
              case_overall_result string,
              case_interpretation string,
              case_test_names string
        )
    """,

    "accession_data": f"""
        CREATE TABLE IF NOT EXISTS {namespace}.accession_data (
            accession_hub_id string,
            icd_codes string,
            cohort_codes string,
            disease_stage_name string,
            disease_type_name string,
            reason_for_referral string,
            disease_status_name string,
            patient_age_at_time_of_service string
        )
    """
}