resource "aws_sfn_state_machine" "etl_orchestration" {
  name     = "etl-orchestration"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "ETL Pipeline Orchestration"
    StartAt = "ProcessLandingToRaw"
    States = {
      "ProcessLandingToRaw" = {
        Type = "Parallel"
        Next = "ProcessRawToStage"
        Branches = [
          {
            StartAt = "ProcessCSVLandingToRaw"
            States = {
              "ProcessCSVLandingToRaw" = {
                Type       = "Task"
                Resource   = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "caylent-poc-etl-landing-to-raw-csv"
                  Arguments = {
                    "--landing_bucket_name"    = "neogenomics-caylent-shared-data-daas"
                    "--datalake_bucket_name"   = "caylent-poc-datalake"
                    "--raw_namespace"          = "raw"
                  }
                }
                End = true
              }
            }
          },
          {
            StartAt = "ProcessFHIRLandingToRaw"
            States = {
              "ProcessFHIRLandingToRaw" = {
                Type       = "Task"
                Resource   = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "caylent-poc-etl-landing-to-raw-fhir"
                }
                End = true
              }
            }
          }
        ]
      },
      "ProcessRawToStage" = {
        Type = "Parallel"
        End  = true
        Branches = [
          {
            StartAt = "ProcessCSVRawToStage"
            States = {
              "ProcessCSVRawToStage" = {
                Type       = "Task"
                Resource   = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "caylent-poc-etl-raw-to-stage-csv"
                }
                End = true
              }
            }
          },
          {
            StartAt = "ProcessFHIRRawToStage"
            States = {
              "ProcessFHIRRawToStage" = {
                Type       = "Task"
                Resource   = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = "caylent-poc-etl-raw-to-stage-fhir"
                }
                End = true
              }
            }
          }
        ]
      }
    }
  })
}

resource "aws_iam_role" "step_function_role" {
  name = "step_function_etl_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name = "step_function_etl_policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "*"
      }
    ]
  })
}