# Configuration Template for Text2SQL Demo
# Copy this file to config.py and fill in your actual values

# AWS Configuration
AWS_REGION = "us-east-1"

# Step Functions Configuration  
DEFAULT_STATE_MACHINE_ARN = "arn:aws:states:us-east-1:664418979226:stateMachine:Text2SQL-dev"

# Prompt Configuration
# These are example prompt IDs - replace with your actual prompt IDs from DynamoDB
PROMPT_IDS = {
    "claude": "036edf14-95c1-45d5-8dd3-5acba379474a",
}

# Default prompt to use
DEFAULT_PROMPT_ID = PROMPT_IDS["claude"]

# Sample queries for testing
SAMPLE_QUERIES = [
    "Filter data by gene, histology, staging, treatment line.",
    "What are the unique gene names?"
] 
