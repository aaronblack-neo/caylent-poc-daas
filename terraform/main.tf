terraform {
  backend "s3" {
    bucket         = "caylent-poc-terraform-state"
    key            = "terraform.tfstate"
    region         = "us-east-1" # You may want to adjust this region
    dynamodb_table = "caylent-poc-terraform-lock"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=5.71.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-east-1" # You may want to adjust this region
}