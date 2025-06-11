

### Scripts to copy data from shared S3 bucket to landing zone, grouping files by type in folders
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
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/orders_fact_data --recursive --exclude "*" --include "ORDERS_FACT_DATA_*"




### Running Glue Jobs (landing -> raw)
aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"accession_data"}'

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"case_data"}'

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"client_data"}'

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"doctor_data"}'

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"image_data"}'

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"orders_fact_data"}'