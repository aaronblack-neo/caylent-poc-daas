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
    "What are the unique gene names?",
    "What are the unique gene names, the first and last order date and the distinct count of patients where the panel is like Myeloid?",
    "What are the unique gene names, the first and last order date and the distinct count of patients where the panel is like Myeloid, the variant classification is pathogenic and the allele frequency > 20?",

    "What are the top 5 examples where the panel is like Myeloid, the variant classification is pathogenic and the allele frequency > 20%, order it by allele frequency ascending?",
"What are the top 5 examples where the panel is like Myeloid, the variant classification is pathogenic and the allele frequency > 20%, order it by allele frequency ascending? Note, the column for allele frequency is a whole number, to do the correct calculation, you need to divide that column by 100%."
] 
