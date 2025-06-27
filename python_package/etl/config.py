landing_bucket_name = "neogenomics-caylent-shared-data-daas"
landing_bucket_filepath = "neogenomics-caylent-shared-data-daas/organized_by_table"
datalake_bucket_name = "caylent-poc-datalake"
namespace = "raw"


raw_s3_tables_schemas = {
    "case_data": {
        "delimiter": "|",
        "create_script": f"""
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
    },
    "accession_data": {
        "delimiter": "|",
        "create_script": f"""
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
        """,
    },
    "client_data": {
        "delimiter": "|",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.client_data (
                client_hub_id string,
                client_number string,
                client_name string,
                client_state string,
                client_state_code string,
                client_postal_code string,
                client_country string,
                client_country_code string,
                client_type string,
                client_specialty string,
                client_setting string
            )
        """,
    },
    "doctor_data": {
        "delimiter": "|",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.doctor_data (
                doctor_hub_id string,
                npi string,
                provider_group string,
                provider_classification string,
                provider_specialization string,
                provider_type string,
                practice_location_state string,
                practice_location_postal_code string,
                practice_location_country string,
                practice_location_country_code string
            )
        """,
    },
    "image_data": {
        "delimiter": "|",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.image_data (
                accession_hub_id string,
                case_hub_id string,
                test_order_hub_id string,
                test_code string,
                test_name string,
                external_image_identifier string,
                external_image_path string,
                image_host_name string,
                image_directory string,
                image_filename string,
                length_in_bytes string,
                file_created_dts_local string,
                file_created_dts_utc string,
                site_code string,
                archive_status string,
                scan_number string,
                scan_type string,
                scan_status_message string,
                scan_instrument string,
                qc_choice string,
                qc_reason string,
                qc_comments string,
                image_reconcile_status string,
                image_quality_tag string,
                icc_profile string,
                image_format string,
                scanner_platform string,
                scan_magnification string,
                image_height string,
                image_width string,
                mpp string
            )
        """,
    },
    "orders_fact_data": {
        "delimiter": "|",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.orders_fact_data (
                accession_hub_id string,
                case_hub_id string,
                test_order_hub_id string,
                gene_name string,
                analysis_performed string,
                panel_code string,
                panel_name string,
                test_code string,
                test_name string,
                exclusion_reasons string,
                ordering_doctor_hub_id string,
                treating_doctor_hub_id string,
                patient_hub_id string,
                client_hub_id string,
                test_order_status string,
                technology_name string,
                technology_std string,
                technique string,
                result_level string,
                result_value string,
                result_value_standard string,
                result_status string,
                nucleotide_change string,
                amino_acid_change string,
                fusion_gene_partners string,
                variant_classification string,
                variant_type string,
                hgvsc string,
                hgvsp string,
                amino_acids string,
                variant_consequence string,
                mutant_allele_frequency string,
                variant_location string,
                variant_location_ordinal string,
                karyotype string,
                test_ordered_timestamp string,
                case_first_signed_timestamp string,
                test_info_exists string
            )
        """,
    },
    "patient_data": {
        "delimiter": "|",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.patient_data (
                patient_hub_id string,
                patient_gender string,
                patient_date_of_birth string,
                patient_age_current string,
                patient_state string,
                patient_postal_code string
            )
        """,
    },
    "patient_match_hub": {
        "delimiter": ",",
        "create_script": f"""
            CREATE TABLE IF NOT EXISTS {namespace}.patient_match_hub (
                id string,
                resourceType string,
                patient_hub_id string
            )
        """,
    },
}
