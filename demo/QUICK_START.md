# 🚀 Text2SQL Demo - Quick Start Guide

**Get the Text2SQL demo running in 2 minutes!**

## 📋 What You Need

1. **Docker installed** on your computer
   - **Windows/Mac**: [Download Docker Desktop](https://www.docker.com/products/docker-desktop)
   - **Linux**: [Install Docker](https://docs.docker.com/engine/install/)

2. **Your AWS settings**
   - **AWS Access Key ID** and **Secret Access Key** (from AWS IAM)
   - **State Machine ARN** from your deployed pipeline
   - **Prompt ID** from your DynamoDB table

## 🎯 Super Easy Start (Recommended)

### Option 1: One-Click Start Scripts

**For Mac/Linux users:**
```bash
chmod +x run.sh
./run.sh
```

**For Windows users:**
Double-click `run.bat`

That's it! The script will:
- ✅ Check if Docker is installed
- ✅ Offer to create a configuration file
- ✅ Build and start the container
- ✅ Open your browser automatically

## 🛠️ Manual Docker Commands (Alternative)

If you prefer to run Docker commands manually:

### 1. Build and Start
```bash
docker-compose up --build -d
```

### 2. Open Your Browser
Go to: http://localhost:8501

### 3. Stop the App
```bash
docker-compose down
```

## ⚙️ Configuration Options

### Easy Way: Use the Web Interface
After starting the app, configure everything through the sidebar in the web interface:
- Enter your **AWS Access Key ID**
- Enter your **AWS Secret Access Key**
- Enter your **State Machine ARN**
- Enter your **Prompt ID**

### Advanced Way: Configuration File
1. Copy the template: `cp env.template .env`
2. Edit `.env` with your AWS credentials and settings:
   ```
   AWS_ACCESS_KEY_ID=your-access-key-here
   AWS_SECRET_ACCESS_KEY=your-secret-key-here
   STATE_MACHINE_ARN=your-state-machine-arn
   DEFAULT_PROMPT_ID=your-prompt-id
   ```
3. Restart: `docker-compose restart`

## 🔐 Getting Your AWS Credentials

### AWS Access Key ID & Secret Access Key
1. Go to [AWS Console](https://console.aws.amazon.com/) → **IAM** → **Users**
2. Find your user and click on it
3. Go to **Security credentials** tab
4. Click **Create access key**
5. Copy the **Access Key ID** and **Secret Access Key**

⚠️ **Important**: Keep your secret access key secure and never share it publicly!

### State Machine ARN
Find your Step Functions ARN:
```bash
aws stepfunctions list-state-machines --region us-west-2
```

### Prompt ID  
Find your prompt ID from DynamoDB:
```bash
aws dynamodb scan --table-name your-prompts-table --region us-west-2
```

## 🔧 Common Commands

| Action | Command |
|--------|---------|
| Start the app | `docker-compose up -d` |
| Stop the app | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Restart | `docker-compose restart` |
| Rebuild | `docker-compose up --build -d` |

## 🆘 Troubleshooting

**App won't start?**
- Check Docker is running: `docker version`
- View logs: `docker-compose logs`

**Can't access the web interface?**
- Make sure you're going to: http://localhost:8501
- Check if container is running: `docker ps`

**AWS connection issues?**
- Verify your Access Key ID and Secret Access Key are correct
- Check that your IAM user has permissions for Step Functions
- Ensure your State Machine ARN is correct and in the right region

**"Failed to initialize AWS client" error?**
- Double-check your AWS credentials in the sidebar
- Make sure your IAM user has `stepfunctions:ListStateMachines` permission
- Verify the AWS region matches where your resources are deployed

## 🎉 You're Ready!

Once running, you can:
1. **Configure your AWS settings** in the sidebar (Access Key, Secret Key, etc.)
2. **Ask questions** using the text area or quick query buttons
3. **View results** with formatted summaries, data, and sources
4. **Manage sessions** to maintain conversation context

**Need help?** Check the full README.md for detailed documentation.

---

**🎯 Goal**: Get you up and running with minimal technical knowledge required! 