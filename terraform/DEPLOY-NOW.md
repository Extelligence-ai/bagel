# Deploy Bagel to ECS - Quick Start

## Current Status

❌ **Bagel is NOT running yet** - We've only created the Terraform configuration.

## Deploy in 3 Steps

### Step 1: Deploy Infrastructure

```bash
cd terraform
terraform init
terraform plan  # Review what will be created
terraform apply  # Type 'yes' when prompted
```

**This creates:**
- ✅ ECR repository
- ✅ S3 bucket
- ✅ ECS cluster
- ✅ ECS service (but no tasks running yet - no image)
- ✅ IAM roles
- ✅ Security groups
- ✅ CloudWatch logs

**Time**: ~5-10 minutes

### Step 2: Build and Push Docker Image

After infrastructure is created:

```bash
# Get ECR repository URL
ECR_URL=$(terraform output -raw ecr_repository_url)
echo "ECR URL: $ECR_URL"

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_URL

# Build the image
docker build \
  -f ../docker/Dockerfile.ecs \
  --build-arg DEV_MODE=false \
  --build-arg BAGEL_LOCAL_HOST=0.0.0.0 \
  --build-arg BAGEL_MCP_LOCAL_PORT=8000 \
  -t $ECR_URL:latest \
  ..

# Push the image
docker push $ECR_URL:latest
```

**Time**: ~10-15 minutes (depending on image size)

### Step 3: Start ECS Service

```bash
# Force service to use the new image
CLUSTER=$(terraform output -raw ecs_cluster_name)
SERVICE=$(terraform output -raw ecs_service_name)

aws ecs update-service \
  --cluster $CLUSTER \
  --service $SERVICE \
  --force-new-deployment \
  --region us-east-1

# Wait for service to start (2-3 minutes)
aws ecs wait services-stable \
  --cluster $CLUSTER \
  --services $SERVICE \
  --region us-east-1
```

**Time**: ~2-3 minutes

## Get Your URL

### Option 1: With Load Balancer (Recommended for Production)

If you enabled load balancer:

```bash
# Get ALB DNS name
terraform output alb_dns_name

# Your URL will be:
# http://<alb-dns-name>:8000
```

### Option 2: Direct ECS Task IP (For Testing)

If no load balancer, get task IP:

```bash
# Get task IP
TASK_ARN=$(aws ecs list-tasks \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service-name $(terraform output -raw ecs_service_name) \
  --query 'taskArns[0]' \
  --output text)

TASK_IP=$(aws ecs describe-tasks \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --tasks $TASK_ARN \
  --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
  --output text | xargs -I {} aws ec2 describe-network-interfaces \
  --network-interface-ids {} \
  --query 'NetworkInterfaces[0].Association.PublicIp' \
  --output text)

echo "MCP Server URL: http://$TASK_IP:8000"
```

**⚠️ Note**: Task IPs change when tasks restart. Use load balancer for stable URL.

## Quick Deploy Script

I'll create an all-in-one deploy script for you!
