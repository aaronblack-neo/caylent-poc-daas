


# Grouping files by type in folders
#ACCESSION_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/accession_data --recursive --exclude "*" --include "ACCESSION_DATA_*"

#CASE_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/case_data --recursive --exclude "*" --include "CASE_DATA_*"

#CLIENT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/client_data --recursive --exclude "*" --include "CLIENT_DATA_*"

#DOCTOR_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/doctor_data --recursive --exclude "*" --include "DOCTOR_DATA_*"

#IMAGE_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/image_data --recursive --exclude "*" --include "IMAGE_DATA_*"

#ORDERS_FACT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/orders_facts_data --recursive --exclude "*" --include "ORDERS_FACT_DATA_*"