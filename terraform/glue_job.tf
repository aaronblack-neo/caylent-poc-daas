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
  description = "Processes CSV files - Source: Landing zone S3 bucket, Target: Raw zone in parquet format"


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
  description = "Processes FHIR JSON files - Source: Landing zone S3 bucket, Target: Raw zone in parquet format"

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
  description = "Transforms raw FHIR data - Source: Raw zone parquet files, Target: Stage zone, Functions: FHIR transformations / parsing"

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
  description = "Transforms raw CSV tables - Source: Raw zone parquet files, Target: Stage zone"

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

# Glue Job for Comprehend

resource "aws_s3_object" "comprehend_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.comprehend_job_name}"
  source = "../python_package/glue_jobs/${local.comprehend_job_name}"
  etag   = filemd5("../python_package/glue_jobs/${local.comprehend_job_name}")
}

resource "aws_glue_job" "comprehend_job" {
  name     = "caylent-poc-etl-comprehend"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.comprehend_job_script.key}"
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
    "--input_s3_path"                    = local.input_s3_path
    "--output_s3_txt_path"               = local.output_s3_txt_path
    "--output_s3_comprehend_path"        = local.output_s3_comprehend_path
    "--additional-python-modules"        = "openpyxl==3.1.2"
      }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}

# Medication statement glue job

resource "aws_s3_object" "medication_statement_job_script" {
  bucket = aws_s3_bucket.glue_scripts_bucket.id
  key    = "glue_jobs/${local.medication_statement}"
  source = "../python_package/glue_jobs/${local.medication_statement}"
  etag   = filemd5("../python_package/glue_jobs/${local.medication_statement}")
}

resource "aws_glue_job" "medication_statement_job" {
  name     = "patient_statements_etl"
  role_arn = aws_iam_role.glue_etl_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts_bucket.id}/${aws_s3_object.medication_statement_job_script.key}"
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
    "--INPUT_S3_PATH"                    = local.INPUT_S3_PATH
    "--OUTPUT_S3_PATH"                   = local.OUTPUT_S3_PATH
      }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = "2"

  execution_property {
    max_concurrent_runs = 10
  }
}