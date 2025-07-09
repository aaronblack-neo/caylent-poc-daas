resource "aws_dynamodb_table" "prompts-table" {
  name           = "Text2SQL_Prompts_terraform"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ID"
  range_key      = "Timestamp"

  attribute {
    name = "ID"
    type = "S"
  }

  attribute {
    name = "Timestamp"
    type = "S"
  }

  attribute {
    name = "Session_ID"
    type = "S"
  }

  global_secondary_index {
    name               = "Session-ID-Timestamp-index"
    hash_key           = "Session_ID"
    range_key          = "Timestamp"
    projection_type    = "ALL"
  }
}

resource "aws_dynamodb_table" "conversation-table" {
  name           = "Text2SQL_Conversations_terraform"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ID"
  range_key      = "Timestamp"

  attribute {
    name = "ID"
    type = "S"
  }

  attribute {
    name = "Timestamp"
    type = "S"
  }

  attribute {
    name = "Session_ID"
    type = "S"
  }

  global_secondary_index {
    name               = "Session-ID-Timestamp-index"
    hash_key           = "Session_ID"
    range_key          = "Timestamp"
    projection_type    = "ALL"
  }
}

resource "aws_dynamodb_table" "caylent-table" {
  name           = "caylent-poc-terraform-lock_terraform"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

}