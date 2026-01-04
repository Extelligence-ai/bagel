# Quick Start: Deploy and Get URL

## Current Status

❌ **Bagel is NOT running** - You need to deploy first!

## 🚀 Deploy in 3 Commands

### Option 1: Automated (Easiest)

```bash
cd terraform
./deploy-complete.sh
```

This does everything and shows you the URL at the end!

### Option 2: Manual

```bash
# 1. Deploy infrastructure
cd terraform
terraform init
terraform apply

# 2. Build and push image (in new terminal or after apply)
ECR_URL=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ECR_URL
docker build -f ../docker/Dockerfile.ecs -t $ECR_URL:latest ..
docker push $ECR_URL:latest

# 3. Start service
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --force-new-deployment
```

## 📡 Get Your URL

### With Load Balancer (Recommended - Stable URL)

I've enabled the load balancer in your config. After deployment:

```bash
terraform output alb_dns_name
# Output: bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com

# Your URL:
# http://bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com:8000
```

**✅ This URL is stable and won't change!**

### Without Load Balancer (Task IP - Changes on Restart)

```bash
# Get task IP (see GET-URL.md for full script)
# ⚠️ This IP changes when tasks restart
```

## 🔗 Use in Your Other Project

### Python Example

```python
# Your Bagel MCP Server URL
BAGEL_URL = "http://bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com:8000"

# Make request
import requests

response = requests.post(
    f"{BAGEL_URL}/tools/analyze_trajectory",
    headers={"Content-Type": "application/json"},
    json={"robolog_path": "s3://bucket/path/to/file.bag"}
)
```

### Environment Variable

```bash
# In your other project's .env
BAGEL_MCP_URL=http://bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com:8000
```

## ⚡ Quick Deploy Right Now

```bash
cd terraform
./deploy-complete.sh
```

Wait ~15-20 minutes, then you'll have your URL! 🚀
