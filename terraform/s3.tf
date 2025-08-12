

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

# Medication statement bucket

resource "aws_s3_bucket" "medication_statement_bucket" {
  bucket = "healthlake-poc-sample"
}



resource "aws_s3_object" "python_lib" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "artifacts/${local.python_lib_file}"
  source = "../files/${local.python_lib_file}"
  etag   = filemd5("../files/${local.python_lib_file}")
}

resource "aws_s3_object" "s3_table" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "s3_tables_jars/${local.s3_table_file}"
  source = "../files/${local.s3_table_file}"
  etag   = filemd5("../files/${local.s3_table_file}")
}