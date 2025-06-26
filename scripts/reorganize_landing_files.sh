

### Scripts to copy data from shared S3 bucket to landing zone, grouping files by type in folders
#ACCESSION_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/accession_data --recursive --exclude "*" --include "ACCESSION_DATA_*"

#CASE_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/case_data --recursive --exclude "*" --include "CASE_DATA_*"

#CLIENT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/client_data --recursive --exclude "*" --include "CLIENT_DATA_*"

#DOCTOR_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/doctor_data --recursive --exclude "*" --include "DOCTOR_DATA_*"

#IMAGE_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/image_data --recursive --exclude "*" --include "IMAGE_DATA_*"

#ORDERS_FACT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/orders_fact_data --recursive --exclude "*" --include "ORDERS_FACT_DATA_*"


#PATIENT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://neogenomics-caylent-shared-data-daas/organized_by_table/patient_data --recursive --exclude "*" --include "PATIENT_DATA_*"

# patient_match_hub
aws s3 cp s3://neogenomics-caylent-shared-data-daas/FHIR-Extract/share/Patient_match_hub.csv s3://neogenomics-caylent-shared-data-daas/organized_by_table/patient_match_hub --recursive



