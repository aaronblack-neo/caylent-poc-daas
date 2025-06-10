


# create an s3 bucket
# resource "aws_s3_bucket" "landing_bucket" {
#   bucket = "marcos-test-datalake-landing"
# }
#
# resource "aws_s3_bucket" "raw_bucket" {
#   bucket = "marcos-test-datalake-raw"
# }

# glue scripts bucket
resource "aws_s3_bucket" "glue_scripts_bucket" {
  bucket = "caylent-poc-glue-scripts"
}

# Athena query results bucket
resource "aws_s3_bucket" "athena_query_results_bucket" {
  bucket = "caylent-poc-athena-query-results"
}