

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
            "states.amazonaws.com", # Added Step Functions as a principal
            "healthlake.amazonaws.com"
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
          "s3:DeleteObject",
          "s3tables:*"
        ]
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.glue_scripts_bucket.id}*",
          "arn:aws:s3:::${aws_s3_bucket.datalake_bucket.id}*",
          "arn:aws:s3:::${local.client_landing_bucket}*",
          "arn:aws:s3tables:us-east-1:${local.account_id}:bucket/*",
          "arn:aws:s3:::494652b6d8cfe38eab542ed7fb4ea898-009160067849-group*"

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

resource "aws_iam_policy" "glue_permissions" {
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
          "glue:UpdateCrawler",
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_step_function_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.glue_permissions.arn
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


## Redshift

# Create an IAM role for Redshift Spectrum
resource "aws_iam_role" "redshift_spectrum_role" {
  name = "redshift_spectrum_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Policy for Redshift Spectrum to access S3 and Glue Data Catalog
resource "aws_iam_policy" "redshift_spectrum_policy" {
  name        = "RedshiftSpectrumAccessPolicy"
  description = "Policy to allow Redshift Spectrum access to S3 and Glue Data Catalog"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:List*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "redshift_spectrum_policy_attachment" {
  role       = aws_iam_role.redshift_spectrum_role.name
  policy_arn = aws_iam_policy.redshift_spectrum_policy.arn
}




# Create ComprehendMedical policy
resource "aws_iam_policy" "comprehend_medical_access" {
  name        = "ComprehendMedicalAccessPolicy"
  description = "Policy to allow access to ComprehendMedical DetectEntitiesV2"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "comprehendmedical:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach the policy to the Glue ETL role
resource "aws_iam_role_policy_attachment" "comprehend_medical_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.comprehend_medical_access.arn
}


resource "aws_iam_policy" "kms_access_policy" {
  name        = "KMSAccessPolicy"
  description = "Policy to allow access to KMS key for Glue ETL role"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:DescribeKey",
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "arn:aws:kms:us-east-1:${local.account_id}:key/6b5a4415-a66c-4eca-a047-9c43e929812a"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kms_access_policy_attachment" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = aws_iam_policy.kms_access_policy.arn
}