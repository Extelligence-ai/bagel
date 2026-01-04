# Next Steps: Deploying Bagel to ECS

## ✅ What's Already Done

- ✅ VPC and subnets discovered and configured
- ✅ AWS account ID auto-detected
- ✅ Terraform configuration complete
- ✅ S3 bucket will be created automatically
- ✅ Multitenancy support configured (optional)

## 🚀 Deployment Steps

### Step 1: Review Configuration

Check your `terraform.tfvars` file:
```bash
cat terraform/terraform.tfvars
```

Everything should be pre-configured! ✅

### Step 2: Initialize Terraform

```bash
cd terraform
terraform init
```

This downloads the AWS provider and sets up Terraform.

### Step 3: Review the Plan

See what Terraform will create:
```bash
terraform plan
```

You should see:
- ✅ ECR repository
- ✅ S3 bucket (for storage)
- ✅ ECS cluster
- ✅ ECS task definition
- ✅ ECS service
- ✅ IAM roles
- ✅ Security groups
- ✅ CloudWatch log group

### Step 4: Apply the Infrastructure

```bash
terraform apply
```

Type `yes` when prompted. This will create all AWS resources.

**Time**: ~5-10 minutes

### Step 5: Build and Push Docker Image

After infrastructure is created:

```bash
# Get ECR URL from Terraform output
ECR_URL=$(terraform output -raw ecr_repository_url)

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

### Step 6: Update ECS Service

Force the service to use the new image:

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)
SERVICE=$(terraform output -raw ecs_service_name)

aws ecs update-service \
  --cluster $CLUSTER \
  --service $SERVICE \
  --force-new-deployment
```

### Step 7: Verify Deployment

Check service status:
```bash
aws ecs describe-services \
  --cluster $CLUSTER \
  --services $SERVICE \
  --query 'services[0].{Status:status,Running:runningCount,Desired:desiredCount}'
```

View logs:
```bash
LOG_GROUP=$(terraform output -raw cloudwatch_log_group)
aws logs tail $LOG_GROUP --follow
```

## 📋 What Gets Created

### Automatically Created:
- ✅ **ECR Repository**: `bagel-mcp-server` (for Docker images)
- ✅ **S3 Bucket**: `bagel-prod-storage-{account-id}` (for robologs, artifacts)
- ✅ **ECS Cluster**: `bagel-cluster`
- ✅ **ECS Service**: `bagel-mcp-service` (long-running)
- ✅ **IAM Roles**: Execution and task roles with proper permissions
- ✅ **Security Groups**: Network access control
- ✅ **CloudWatch Logs**: `/ecs/bagel-mcp-service`

### Optional (if enabled):
- ⚙️ **Application Load Balancer**: If `enable_load_balancer = true`
- ⚙️ **Auto Scaling**: If `enable_auto_scaling = true`
- ⚙️ **Multitenancy**: If `enable_multitenancy = true`

## 🔗 Connecting Your Project

Once deployed, get the endpoint:

```bash
terraform output service_endpoint
```

Then connect from your project:
```python
from ecs.client_example import BagelMCPClient

client = BagelMCPClient("http://your-endpoint:8000")
```

## 📊 Monitoring

### View Logs
```bash
aws logs tail $(terraform output -raw cloudwatch_log_group) --follow
```

### Check Service Health
```bash
aws ecs describe-services \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --services $(terraform output -raw ecs_service_name)
```

### View Metrics
- Go to AWS Console → ECS → Clusters → bagel-cluster
- Click on "Metrics" tab

## 🔧 Multitenancy Setup (Optional)

If you need multitenancy:

1. **Edit `terraform.tfvars`**:
```hcl
enable_multitenancy = true
tenant_names = ["tenant-1", "tenant-2"]
```

2. **Apply changes**:
```bash
terraform apply
```

3. **See `MULTITENANCY.md`** for detailed guide

## 💰 Estimated Costs

### Base Infrastructure (Always On):
- ECS Fargate (512 CPU, 1GB): ~$15-20/month
- ECR Storage: ~$0.10/GB/month
- S3 Storage: ~$0.023/GB/month
- CloudWatch Logs: ~$0.50/GB ingested
- **Total**: ~$20-30/month base

### With Usage:
- Data transfer: ~$0.09/GB
- Additional compute: Scales automatically
- **Total**: ~$30-100/month typical

## 🛠️ Troubleshooting

### Service Won't Start
1. Check CloudWatch logs
2. Verify Docker image was pushed
3. Check IAM role permissions
4. Verify security group allows traffic

### Can't Connect
1. Check security group allows your IP
2. Verify service is running
3. Check health endpoint: `curl http://endpoint:8000/`

### High Costs
1. Review auto-scaling settings
2. Check S3 storage usage
3. Review CloudWatch log retention

## 🎯 Quick Commands Reference

```bash
# Get all outputs
terraform output

# Get specific output
terraform output ecr_repository_url
terraform output s3_bucket_name
terraform output service_endpoint

# View logs
aws logs tail $(terraform output -raw cloudwatch_log_group) --follow

# Scale service
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --desired-count 2

# Destroy everything (careful!)
terraform destroy
```

## ✅ You're Ready!

Everything is configured. Just run:
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Then build and push your Docker image, and you're live! 🚀
