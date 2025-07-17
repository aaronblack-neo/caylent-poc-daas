resource "aws_glue_catalog_database" "raw" {
  name        = local.raw_database_name
  description = "Glue database for raw data"
}

resource "aws_glue_catalog_database" "stage" {
  name        = local.stage_database_name
  description = "Glue database for stage data"
}

