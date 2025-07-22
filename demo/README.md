# Text2SQL Demo

A simple Streamlit app that demonstrates the Text2SQL pipeline by allowing users to ask natural language questions and get SQL-powered answers through AWS Step Functions.

## Features

- **Simple Interface**: Clean web UI for entering questions
- **Real-time Processing**: Watch your query progress through the pipeline
- **Session Management**: Maintain conversation context across multiple queries
- **AWS Integration**: Direct integration with your deployed Step Functions pipeline
- **Response Display**: Formatted display of summaries, data, sources, and links
- **Easy Credentials**: Direct AWS credential input - no AWS CLI setup required

## Prerequisites

1. **Docker** installed on your computer
   - **Windows/Mac**: [Download Docker Desktop](https://www.docker.com/products/docker-desktop)
   - **Linux**: [Install Docker](https://docs.docker.com/engine/install/)

2. **AWS Credentials**: Your AWS Access Key ID and Secret Access Key
3. **Deployed Pipeline**: The Text2SQL Step Functions pipeline must be deployed and running

## Setup

### Option 1: One-Click Start (Easiest)

**For Mac/Linux:**
```bash
chmod +x run.sh
./run.sh
```

**For Windows:**
Double-click `run.bat`

### Option 2: Manual Docker Setup

1. **Build and Start**:
   ```bash
   docker-compose up --build -d
   ```

2. **Open your browser** to `http://localhost:8501`

3. **Configure in the web interface**:
   - AWS Access Key ID
   - AWS Secret Access Key  
   - AWS Region (default: us-west-2)
   - State Machine ARN
   - Prompt ID

## Configuration

### Easy Way: Web Interface (Recommended)
After starting the app, enter all settings through the sidebar:
- **AWS Access Key ID**: Your AWS access key
- **AWS Secret Access Key**: Your AWS secret key
- **State Machine ARN**: Full ARN of your deployed state machine
- **Prompt ID**: The ID of the prompt configuration to use

### Advanced Way: Configuration Files
Create a `.env` file or `config.py` file with your settings:

**Using .env file:**
```bash
cp env.template .env
# Edit .env with your values
```

**Using config.py file:**
```bash
cp config_template.py config.py
# Edit config.py with your values
```

## Getting Your AWS Information

### AWS Credentials
1. Go to [AWS Console](https://console.aws.amazon.com/) → **IAM** → **Users**
2. Find your user → **Security credentials** tab
3. Click **Create access key**
4. Copy the **Access Key ID** and **Secret Access Key**

⚠️ **Security**: Keep your credentials secure and never share them publicly!

### State Machine ARN
Find your Step Functions ARN:
```bash
aws stepfunctions list-state-machines --region us-west-2
```

Or check your CloudFormation stack outputs:
```bash
aws cloudformation describe-stacks --stack-name dev-text2sql-pipeline --region us-west-2 --query 'Stacks[0].Outputs'
```

### Prompt ID
Find available prompt IDs in your DynamoDB prompts table:
```bash
aws dynamodb scan --table-name your-prompts-table --region us-west-2
```

## Usage

1. **Start the app** using one of the setup methods above
2. **Configure credentials** in the sidebar
3. **Ask questions** using the text area or quick query buttons
4. **View results** with formatted summaries, data, and sources

### Sample Questions

- "What leasing deals are expected to close by the end of this quarter?"
- "Show me the top 5 properties by occupancy rate"
- "Which deals have the lowest price?"
- "What is the current occupancy rate for all properties?"

## Features in Detail

### Session Management
- Each session maintains conversation context with a unique UUID
- Click "🔄 Reset Session" to start fresh
- Session ID is displayed and used in all queries

### Quick Queries
- Pre-built sample queries for easy testing
- Click any quick query button to load it into the text area
- Edit queries before executing them

### Progress Tracking
- Real-time progress bar during query execution
- Status updates show current processing stage
- Execution details available in expandable section

### Response Display
- **Summary**: Natural language answer to your question
- **Data**: Structured data results from the SQL query
- **Sources**: Database tables used in the query
- **Links**: Any relevant links or references

## Container Management

| Action | Command |
|--------|---------|
| Start the app | `docker-compose up -d` |
| Stop the app | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Restart | `docker-compose restart` |
| Rebuild | `docker-compose up --build -d` |

## Troubleshooting

### Common Issues

1. **"Failed to initialize AWS client" error**:
   - Verify your Access Key ID and Secret Access Key are correct
   - Make sure your IAM user has `stepfunctions:ListStateMachines` permission
   - Check that the AWS region matches where your resources are deployed

2. **App won't start**:
   - Check Docker is running: `docker version`
   - View logs: `docker-compose logs`

3. **Can't access the web interface**:
   - Make sure you're going to: http://localhost:8501
   - Check if container is running: `docker ps`

4. **Pipeline execution fails**:
   - Check CloudWatch logs for the Lambda functions
   - Verify your prompt ID exists in the prompts table
   - Ensure your RDS/DynamoDB resources are accessible

### Required AWS Permissions

Your IAM user needs these minimum permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution",
                "states:DescribeExecution",
                "states:ListStateMachines"
            ],
            "Resource": "*"
        }
    ]
}
```

## Security Notes

- **Never hardcode credentials** in configuration files that might be committed to version control
- **Use environment variables** or the web interface for credential input
- **Rotate your access keys** regularly
- **Use least privilege** IAM policies
- **Monitor CloudWatch logs** for any security events

## Architecture

The app creates a containerized Streamlit interface that:
1. **Accepts user input** through a clean web interface
2. **Manages AWS credentials** securely without requiring AWS CLI
3. **Calls Step Functions** using direct AWS API calls
4. **Displays results** in a formatted, user-friendly way
5. **Maintains session state** for conversation context

No AWS CLI installation or profile configuration required! 