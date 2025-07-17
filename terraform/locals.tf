locals {
  client_landing_bucket = "neogenomics-caylent-shared-data-daas"
  account_id            = "664418979226"

  # lake formation

  databases = [] #["raw"]
  roles     = [aws_iam_role.glue_etl_role.arn, "arn:aws:iam::664418979226:user/marcos.foglino@caylent.com", "arn:aws:iam::664418979226:user/bruno.souza@caylent.com"]
  databases_with_roles = flatten([
    for role in local.roles : [
      #for database in concat(var.databases_name, ["default"]) : {
      for database in local.databases : {
        role     = role
        database = database
      }
    ]
  ])

  # glue
  
  python_lib_file             = "python_libs-0.1.0-py3-none-any.whl"
  s3_table_file               = "s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
  raw_job_name                = "job_landing_to_raw_csv.py"
  fhir_job_name               = "job_landing_to_raw_fhir.py"
  fhir_stage_job_name         = "job_raw_to_stage_fhir.py"
  csv_stage_job_name          = "job_raw_to_stage_csv.py"
  comprehend_job_name         = "job_comprehend.py"
  medication_statement        = "patient_statements_etl.py"
  bucket_name                 = "caylent-poc-medical-comprehend"
  input_s3_path               = "s3://${aws_s3_bucket.comprehend_bucket.id}/example/input/"
  output_s3_txt_path          = "s3://${aws_s3_bucket.comprehend_bucket.id}/example/output/"
  output_s3_comprehend_path   = "s3://${aws_s3_bucket.comprehend_bucket.id}/example/results/"
  INPUT_S3_PATH               = "s3://${aws_s3_bucket.medication_statement_bucket.id}/medication-statement/"
  OUTPUT_S3_PATH              = "s3://${aws_s3_bucket.medication_statement_bucket.id}/patientstatement_output"

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
