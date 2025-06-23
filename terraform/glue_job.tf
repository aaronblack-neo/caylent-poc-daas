locals {
  raw_job_name = "job_landing_to_raw.py"
  s3_tables_job_name = "job_create_raw_s3_tables.py"
  fhir_job_name = "job_process_fhir_data.py"

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


  s3_tables_spark_conf = <<EOT
 conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
 --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160
 --conf spark.sql.defaultCatalog=s3tablesbucket
 --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog
 --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
 --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3tables_table_bucket.table_bucket.name}
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
  name     = "caylent-poc-etl-landing-to-raw"
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
    "--table_name"                       = "patient_match_hub"
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}


## S3 Tables Job
resource "aws_s3_object" "s3_tables_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.s3_tables_job_name}"
  source = "../python_package/glue_jobs/${local.s3_tables_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.s3_tables_job_name}")
}

resource "aws_glue_job" "s3_tables_job" {
  name     = "caylent-poc-etl-create-s3-tables"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.s3_tables_job_script.key}"
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
    "--conf"                             = trim(local.s3_tables_spark_conf, "\n")
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--landing_bucket_name"              = local.client_landing_bucket
    "--raw_namespace"                  = "raw"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}



resource "aws_s3_object" "fhir_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.fhir_job_name}"
  source = "../python_package/glue_jobs/${local.fhir_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.fhir_job_name}")
}

resource "aws_glue_job" "fhir_job" {
  name     = "caylent-poc-etl-parse-fhir"
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
    #"--extra-py-files"                   = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/artifacts/python_libs-0.1.0-py3-none-any.whl"
    "--conf"                             = trim(local.iceberg_spark_conf, "\n")
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--landing_bucket_name"              = local.client_landing_bucket
    "--raw_namespace"                  = "raw"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}