# Terraform Configuration with S3 Backend and DynamoDB Locking

This repository contains Terraform configuration with remote state storage in S3 and state locking using DynamoDB.

## Prerequisites

Before using this configuration, ensure you have:

1. AWS credentials configured
2. The following AWS resources:
   - S3 bucket: `caylent-poc-terraform-state`
   - DynamoDB table: `caylent-poc-terraform-lock` (with partition key: `LockID` of type String)

## Configuration Details

- Backend Type: S3
- State Bucket: caylent-poc-terraform-state
- Lock Table: caylent-poc-terraform-lock
- Default Region: us-east-1 (configurable via variables)

## Usage

1. Initialize Terraform:
   ```
   terraform init
   ```

2. Plan your changes:
   ```
   terraform plan
   ```

3. Apply the configuration:
   ```
   terraform apply
   ```

## Building Python Package
To build the Python package, follow these steps (locally or in a CI/CD pipeline):
1. Ensure you have Python and pip installed.
2. Navigate to the directory containing the `setup.py` file.
3. Run the following command to build and deploy the package:
   ```
   python setup.py bdist_wheel --dist-dir /tmp/python_artifact;  aws s3 cp /tmp/python_artifact/python_libs-0.1.0-py3-none-any.whl  s3://caylent-poc-glue-scripts/artifacts/; rm -rf python_libs.egg-info; rm -rf build; 
   ```


## Important Notes

- Ensure you have appropriate permissions to access the S3 bucket and DynamoDB table
- The state file is encrypted at rest in the S3 bucket
- Make sure to properly configure AWS credentials before running Terraform commands