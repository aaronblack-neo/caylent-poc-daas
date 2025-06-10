



resource "aws_s3_bucket" "raw_bucket" {
  bucket = "caylent-poc-dl-raw"
}

# glue scripts bucket
resource "aws_s3_bucket" "glue_scripts_bucket" {
  bucket = "caylent-poc-glue-scripts"
}

# Athena query results bucket
resource "aws_s3_bucket" "athena_query_results_bucket" {
  bucket = "caylent-poc-athena-query-results"
}