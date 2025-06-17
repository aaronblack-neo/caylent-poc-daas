

# resource "aws_s3tables_table" "example" {
#   name             = "example_table"
#   namespace        = aws_s3tables_namespace.example.namespace
#   table_bucket_arn = aws_s3tables_namespace.example.table_bucket_arn
#   format           = "ICEBERG"
# }


resource "aws_s3tables_table_bucket" "table_bucket" {
  name = "caylent-poc-table-bucket"
}

resource "aws_s3tables_namespace" "table_bucket_namespace" {
  namespace        = "caylent_poc_table_bucket_namespace"
  table_bucket_arn = aws_s3tables_table_bucket.table_bucket.arn
}
