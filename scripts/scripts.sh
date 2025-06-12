

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

#PATIENT_DATA
aws s3 cp s3://neogenomics-caylent-shared-data-daas s3://caylent-poc-dl-landing/patient_data --recursive --exclude "*" --include "PATIENT_DATA_*"


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

aws glue start-job-run \
  --job-name caylent-poc-etl-landing-to-raw \
  --arguments '{"--landing_bucket_name":"caylent-poc-dl-landing","--raw_bucket_name":"caylent-poc-dl-raw","--table_name":"patient_data"}'




  select *
  from orders_fact_data ofd
  join accession_data ad
         on ofd.accession_hub_id = ad.accession_hub_id
  join case_data cd
         on ofd.case_hub_id = cd.case_hub_id
  join patient_data pd
         on ofd.patient_hub_id = pd.patient_hub_id
  join client_data cld
         on ofd.client_hub_id = cld.client_hub_id
  join doctor_data odd
         on ofd.ordering_doctor_hub_id = odd.doctor_hub_id
  join doctor_data tdd
         on ofd.treating_doctor_hub_id = tdd.doctor_hub_id
  join image_data id
         on ofd.test_order_hub_id = id.test_order_hub_id
  join client_data cli
         on ofd.client_hub_id = cli.client_hub_id
  limit 10