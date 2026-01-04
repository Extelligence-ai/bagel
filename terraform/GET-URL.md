# Getting Your Bagel MCP Server URL

## Current Status

❌ **Bagel is NOT running yet** - You need to deploy first!

## Quick Deploy & Get URL

### Option 1: Automated (Easiest)

```bash
cd terraform
./deploy-complete.sh
```

This will:
1. ✅ Deploy infrastructure
2. ✅ Build Docker image
3. ✅ Push to ECR
4. ✅ Start ECS service
5. ✅ Show you the URL

### Option 2: Manual Steps

#### Step 1: Deploy Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

#### Step 2: Build & Push Image

```bash
ECR_URL=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_URL

docker build -f ../docker/Dockerfile.ecs -t $ECR_URL:latest ..
docker push $ECR_URL:latest
```

#### Step 3: Start Service

```bash
aws ecs update-service \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --force-new-deployment
```

#### Step 4: Get URL

See methods below ⬇️

## Getting the URL

### Method 1: Load Balancer (Recommended)

**If you enabled load balancer:**

```bash
terraform output alb_dns_name
# Output: bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com

# Your URL:
# http://bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com:8000
```

**To enable load balancer:**
```hcl
# terraform.tfvars
enable_load_balancer = true
terraform apply
```

### Method 2: ECS Task IP (Temporary)

**If no load balancer:**

```bash
# Get task IP
CLUSTER=$(terraform output -raw ecs_cluster_name)
SERVICE=$(terraform output -raw ecs_service_name)

TASK_ARN=$(aws ecs list-tasks \
  --cluster $CLUSTER \
  --service-name $SERVICE \
  --query 'taskArns[0]' \
  --output text)

ENI_ID=$(aws ecs describe-tasks \
  --cluster $CLUSTER \
  --tasks $TASK_ARN \
  --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
  --output text)

TASK_IP=$(aws ec2 describe-network-interfaces \
  --network-interface-ids $ENI_ID \
  --query 'NetworkInterfaces[0].Association.PublicIp' \
  --output text)

echo "http://$TASK_IP:8000"
```

**⚠️ Warning**: Task IPs change when tasks restart. Use load balancer for production!

### Method 3: Use Terraform Output

```bash
terraform output service_endpoint
```

## Connecting Your Other Project

### Python Example

```python
# In your other project
import requests

# Your Bagel MCP Server URL
BAGEL_URL = "http://your-alb-dns-name:8000"  # Or task IP

# Make request
response = requests.post(
    f"{BAGEL_URL}/tools/analyze_trajectory",
    headers={
        "Authorization": f"Bearer {cognito_token}",  # If using Cognito
        "Content-Type": "application/json"
    },
    json={
        "robolog_path": "s3://bucket/path/to/robolog.bag"
    }
)
```

### JavaScript/TypeScript Example

```typescript
// In your other project
const BAGEL_URL = "http://your-alb-dns-name:8000";

const response = await fetch(`${BAGEL_URL}/tools/analyze_trajectory`, {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${cognitoToken}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    robolog_path: "s3://bucket/path/to/robolog.bag"
  })
});
```

### MCP Client Example

```python
from mcp import ClientSession
import httpx

# Connect to Bagel MCP Server
async with httpx.AsyncClient() as client:
    # For SSE transport
    async with client.stream(
        'GET',
        f"{BAGEL_URL}/sse",
        headers={"Authorization": f"Bearer {token}"}
    ) as response:
        # Handle SSE stream
        async for line in response.aiter_lines():
            # Process MCP messages
            pass
```

## Recommended Setup for Production

### Enable Load Balancer

```hcl
# terraform.tfvars
enable_load_balancer = true
```

**Benefits:**
- ✅ Stable URL (doesn't change)
- ✅ SSL/TLS support (HTTPS)
- ✅ Health checks
- ✅ Multiple tasks behind one URL

### Get Stable URL

```bash
# After enabling load balancer and deploying
terraform output alb_dns_name

# Your permanent URL:
# http://bagel-mcp-alb-1234567890.us-east-1.elb.amazonaws.com:8000
```

## Quick Status Check

```bash
# Check if service is running
aws ecs describe-services \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --services $(terraform output -raw ecs_service_name) \
  --query 'services[0].{Status:status,Running:runningCount,Desired:desiredCount}'

# Check logs
aws logs tail $(terraform output -raw cloudwatch_log_group) --follow
```

## Next Steps

1. **Deploy**: Run `./deploy-complete.sh` or manual steps
2. **Get URL**: Use `terraform output` or methods above
3. **Test**: Try connecting from your other project
4. **Configure**: Add URL to your other project's config

## Summary

**To get your URL:**

1. **Deploy first**: `cd terraform && ./deploy-complete.sh`
2. **Get URL**: 
   - With ALB: `terraform output alb_dns_name`
   - Without ALB: Get task IP (see Method 2 above)
3. **Use in your project**: `http://your-url:8000`

**Recommended**: Enable load balancer for a stable, production-ready URL! 🚀
