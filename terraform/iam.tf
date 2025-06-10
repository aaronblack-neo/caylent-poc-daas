

# create an IAM role for Glue
resource "aws_iam_role" "glue_etl_role" {
  name = "glue_etl_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "states.amazonaws.com" # Added Step Functions as a principal
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


# give glue role permissions over marcos-test-datalake-glue-scripts
resource "aws_iam_policy" "glue_scripts_access" {
  name        = "GlueScriptsAccessPolicy"
  description = "Policy to allow Glue role access to marcos-test-datalake-glue-scripts bucket"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.glue_scripts_bucket.id}*",
          "arn:aws:s3:::${aws_s3_bucket.landing_bucket.id}*",
          "arn:aws:s3:::${aws_s3_bucket.raw_bucket.id}*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.glue_scripts_access.arn
}


resource "aws_iam_policy" "glue_etl_access" {
  name        = "GlueETLAccessPolicy"
  description = "Policy to allow Glue role access to Glue Data Catalog and S3 bucket"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "iam:PassRole",
          "dynamodb:*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_etl_role_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.glue_etl_access.arn
}


resource "aws_iam_role_policy_attachment" "glue_service_role_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_step_function_access" {
  name        = "GlueStepFunctionAccessPolicy"
  description = "Policy to allow Step Functions to start Glue jobs and manage crawlers"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobs",
          "glue:BatchGetJobs",
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlers",
          "glue:StopCrawler",
          "glue:UpdateCrawler"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_step_function_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.glue_step_function_access.arn
}

resource "aws_iam_policy" "step_function_execution_access" {
  name        = "StepFunctionExecutionAccessPolicy"
  description = "Policy to allow Glue role to execute Step Functions"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution",
          "states:DescribeExecution",
          "states:ListExecutions",
          "states:GetExecutionHistory"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_function_execution_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.step_function_execution_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_lake_formation_admin_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"
}