# Bagel MCP Server - Terraform Deployment

This directory contains Terraform configurations to deploy Bagel MCP Server to AWS ECS as a long-running service.

## Architecture

The Terraform configuration creates:

- **ECR Repository**: Container image repository
- **ECS Cluster**: Container orchestration
- **ECS Task Definition**: Container configuration
- **ECS Service**: Long-running service
- **IAM Roles**: Execution and task roles
- **Security Groups**: Network security
- **CloudWatch Logs**: Log aggregation
- **Application Load Balancer** (optional): Public endpoint
- **Auto Scaling** (optional): Automatic scaling based on metrics

## Prerequisites

1. **Terraform** >= 1.0 installed
2. **AWS CLI** configured with appropriate credentials
3. **AWS Account** with permissions for:
   - ECS, ECR, IAM, VPC, CloudWatch, ALB
4. **VPC and Subnets** already created

## Quick Start

### 1. Find Your VPC and Subnets (First Time?)

**Don't know what VPC/subnets are?** See [README-VPC.md](./README-VPC.md) for a detailed explanation!

**Quick way to find them:**
```bash
cd terraform
./find-vpc-info.sh
```

This script will automatically find your default VPC and subnets and show you what to put in `terraform.tfvars`!

### 2. Configure Variables

Copy the example variables file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your values. If you ran `find-vpc-info.sh`, it will show you the exact values to use:

```hcl
aws_region = "us-east-1"
vpc_id = "vpc-xxxxxxxxx"           # Your VPC ID (from find-vpc-info.sh)
subnet_ids = ["subnet-xxx", "subnet-yyy"]  # Your subnet IDs (from find-vpc-info.sh)
```

### 2. Initialize Terraform

```bash
cd terraform
terraform init
```

### 3. Review Plan

```bash
terraform plan
```

### 4. Apply Configuration

```bash
terraform apply
```

### 5. Build and Push Docker Image

After infrastructure is created, build and push your Docker image:

```bash
# Get ECR repository URL from outputs
ECR_URL=$(terraform output -raw ecr_repository_url)

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_URL

# Build image
docker build \
  -f ../docker/Dockerfile.ecs \
  --build-arg DEV_MODE=false \
  -t $ECR_URL:latest \
  ..

# Push image
docker push $ECR_URL:latest
```

### 6. Update ECS Service

After pushing the image, update the ECS service to use the new image:

```bash
# Force new deployment
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --force-new-deployment
```

## Configuration Options

### Basic Configuration

```hcl
# Minimal configuration
vpc_id = "vpc-xxx"
subnet_ids = ["subnet-xxx", "subnet-yyy"]
```

### With Load Balancer

```hcl
enable_load_balancer = true
allowed_cidr_blocks = ["10.0.0.0/8"]  # Restrict access
```

### Custom Resources

```hcl
task_cpu = 1024      # 1 vCPU
task_memory = 2048   # 2 GB
desired_count = 2
```

### Auto Scaling

```hcl
enable_auto_scaling = true
min_capacity = 1
max_capacity = 20
target_cpu_utilization = 70
target_memory_utilization = 80
```

## Outputs

After deployment, get important values:

```bash
# ECR repository URL
terraform output ecr_repository_url

# Service endpoint
terraform output service_endpoint

# CloudWatch log group
terraform output cloudwatch_log_group
```

## Connecting Your Project

Once deployed, connect to the service:

```python
from ecs.client_example import BagelMCPClient

# If using load balancer
client = BagelMCPClient("http://your-alb-dns-name:8000")

# Or if using direct ECS task IP
client = BagelMCPClient("http://<task-ip>:8000")
```

## Updating the Deployment

### Update Infrastructure

```bash
terraform plan
terraform apply
```

### Update Container Image

1. Build and push new image (see step 5 above)
2. Force ECS service deployment:

```bash
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --force-new-deployment
```

## Scaling

### Manual Scaling

```bash
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --desired-count 3
```

### Auto Scaling

Auto scaling is configured via Terraform. Update variables:

```hcl
min_capacity = 2
max_capacity = 20
```

Then apply:

```bash
terraform apply
```

## Monitoring

### View Logs

```bash
LOG_GROUP=$(terraform output -raw cloudwatch_log_group)
aws logs tail $LOG_GROUP --follow
```

### View Service Status

```bash
aws ecs describe-services \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --services $(terraform output -raw ecs_service_name)
```

## Cleanup

To destroy all resources:

```bash
terraform destroy
```

**Warning**: This will delete all resources including ECR images!

## Advanced Configuration

### Remote State

Uncomment and configure in `main.tf`:

```hcl
backend "s3" {
  bucket = "your-terraform-state-bucket"
  key    = "bagel/ecs/terraform.tfstate"
  region = "us-east-1"
}
```

### Multiple Environments

Use workspaces:

```bash
terraform workspace new dev
terraform workspace new prod
terraform workspace select dev
terraform apply
```

### Custom IAM Policies

Edit `iam.tf` to add additional permissions for:
- S3 access
- Secrets Manager
- Other AWS services

## Troubleshooting

### Service Not Starting

1. Check CloudWatch logs
2. Verify task definition and image URI
3. Check security group allows traffic
4. Verify IAM roles have correct permissions

### Connection Issues

1. Verify security group allows traffic from source
2. Check if service is running
3. Test health endpoint

### High Costs

1. Use Fargate Spot (modify task definition)
2. Right-size CPU and memory
3. Enable auto-scaling to scale down
4. Review CloudWatch Insights

## Security Best Practices

1. **Restrict CIDR blocks** in production
2. **Use Application Load Balancer** with SSL/TLS
3. **Enable VPC Flow Logs**
4. **Use Secrets Manager** for sensitive data
5. **Enable CloudTrail** for audit logging
6. **Regularly update** container images

## Cost Optimization

- Use **Fargate Spot** for non-critical workloads
- Right-size resources based on actual usage
- Enable **auto-scaling** to scale down during low usage
- Use **CloudWatch Insights** to optimize
- Set **log retention** appropriately (default: 7 days)
