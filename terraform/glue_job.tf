locals {
  raw_job_name        = "job_landing_to_raw_csv.py"
  fhir_job_name       = "job_landing_to_raw_fhir.py"
  fhir_stage_job_name = "job_raw_to_stage_fhir.py"
  csv_stage_job_name  = "job_raw_to_stage_csv.py"

  iceberg_spark_conf = <<EOT
 conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
 --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
 --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.datalake_bucket.id}/datalake
 --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
 --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
 --conf spark.sql.defaultCatalog=glue_catalog
 --conf spark.sql.catalog.glue_catalog.default-namespace=raw
 --conf spark.sql.parquet.mergeSchema=true
EOT
}

# Glue ETL Job to move data from Landing to Raw
resource "aws_s3_object" "raw_glue_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.raw_job_name}"
  source = "../python_package/glue_jobs/${local.raw_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.raw_job_name}")
}

resource "aws_glue_job" "raw_job" {
  name     = "caylent-poc-etl-landing-to-raw-csv"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.raw_glue_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--landing_bucket_name"              = local.client_landing_bucket
    "--datalake_bucket_name"             = aws_s3_bucket.datalake_bucket.id
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}


# FHIR Job to process FHIR data
resource "aws_s3_object" "fhir_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.fhir_job_name}"
  source = "../python_package/glue_jobs/${local.fhir_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.fhir_job_name}")
}

resource "aws_glue_job" "fhir_job" {
  name     = "caylent-poc-etl-landing-to-raw-fhir"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.fhir_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--landing_bucket_name"              = local.client_landing_bucket
    "--raw_namespace"                    = "raw"
  }

  glue_version      = "5.0"
  worker_type       = "G.2X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}


# FHIR Stage Job to Parse FHIR data
resource "aws_s3_object" "fhir_stage_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.fhir_stage_job_name}"
  source = "../python_package/glue_jobs/${local.fhir_stage_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.fhir_stage_job_name}")
}

resource "aws_glue_job" "fhir_stage_job" {
  name     = "caylent-poc-etl-raw-to-stage-fhir"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.fhir_stage_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--landing_bucket_name"              = local.client_landing_bucket
    "--namespace"                        = "stage"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}


# CSV Stage Job to process CSV data
resource "aws_s3_object" "csv_stage_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.csv_stage_job_name}"
  source = "../python_package/glue_jobs/${local.csv_stage_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.csv_stage_job_name}")
}

resource "aws_glue_job" "csv_stage_job" {
  name     = "caylent-poc-etl-raw-to-stage-csv"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.csv_stage_job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--namespace"                        = "stage"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}