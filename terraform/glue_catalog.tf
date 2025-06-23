resource "aws_glue_catalog_database" "raw" {
  name        = "raw"
  description = "Glue database for raw data"
}

resource "aws_glue_catalog_database" "stage" {
  name        = "stage"
  description = "Glue database for stage data"
}

