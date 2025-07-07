

resource "aws_s3_bucket" "datalake_bucket" {
  bucket = "caylent-poc-datalake"
}

# glue scripts bucket
resource "aws_s3_bucket" "glue_scripts_bucket" {
  bucket = "caylent-poc-glue-scripts"
}

# Athena query results bucket
resource "aws_s3_bucket" "athena_query_results_bucket" {
  bucket = "caylent-poc-athena-query-results"
}

# Comprehend bucket
resource "aws_s3_bucket" "comprehend_bucket" {
  bucket = "caylent-poc-comprehend-bucket"
}

# create folders in raw bucket
# resource "aws_s3_object" "landing_bucket_folders" {
#   for_each = toset([
#     "accession_data/",
#     "case_data/",
#     "client_data/",
#     "doctor_data/",
#     "image_data/",
#     "orders_fact_data/",
#     "patient_data/"
#   ])
#
#   bucket = aws_s3_bucket.datalake_bucket.id
#   key    = each.value
# }