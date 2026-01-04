# Bagel MCP Server - ECS Deployment Guide

This directory contains files and scripts for deploying Bagel's MCP Server to AWS ECS as a long-running service.

## Overview

The Bagel MCP Server provides trajectory analysis and robotics log processing capabilities via the Model Context Protocol (MCP). This deployment setup allows you to run it as a scalable, long-running service on AWS ECS.

## Prerequisites

1. **AWS CLI** installed and configured
2. **Docker** installed locally
3. **AWS Account** with appropriate permissions:
   - ECS permissions
   - ECR permissions
   - IAM role creation permissions
4. **ECS Cluster** created (or use default)
5. **VPC and Networking** configured:
   - Subnets for Fargate tasks
   - Security groups allowing inbound traffic on port 8000

## Architecture

```
┌─────────────────┐
│   Your Project  │
│  (MCP Client)   │
└────────┬────────┘
         │ MCP Protocol (HTTP/SSE)
         │ Port 8000
         ▼
┌─────────────────┐
│  Application    │
│  Load Balancer  │
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   ECS Service   │
│  (Fargate)      │
│  ┌───────────┐ │
│  │ Bagel MCP  │ │
│  │  Server    │ │
│  └───────────┘ │
└─────────────────┘
```

## Quick Start

### 1. Configure Environment Variables

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=123456789012
export ECR_REPO_NAME=bagel-mcp-server
export ECS_CLUSTER_NAME=bagel-cluster
export ECS_SERVICE_NAME=bagel-mcp-service
```

### 2. Update Task Definition

Edit `ecs/task-definition.json` and replace:
- `YOUR_ACCOUNT_ID` with your AWS account ID
- `YOUR_ECR_REPO_URI` will be auto-replaced by deploy script

### 3. Build and Deploy

```bash
# Make scripts executable
chmod +x ecs/deploy.sh ecs/create-service.sh

# Deploy to ECS
./ecs/deploy.sh
```

### 4. Create ECS Service (First Time Only)

```bash
# Update these values in the script or export as env vars:
export SUBNET_IDS="subnet-12345,subnet-67890"
export SECURITY_GROUP_ID="sg-12345"

# Create the service
./ecs/create-service.sh
```

## Configuration

### Environment Variables

The following environment variables can be set in the task definition:

- `BAGEL_LOCAL_HOST`: Host to bind to (default: `0.0.0.0`)
- `BAGEL_MCP_LOCAL_PORT`: Port for MCP server (default: `8000`)
- `BAGEL_USE_CACHE`: Enable caching (default: `true`)
- `BAGEL_CACHE_DIRECTORY`: Cache directory path
- `BAGEL_STORAGE_DIRECTORY`: Storage directory path

### Resource Requirements

Default task definition uses:
- **CPU**: 512 (0.5 vCPU)
- **Memory**: 1024 MB (1 GB)

Adjust these in `task-definition.json` based on your workload:
- Light usage: 256 CPU, 512 MB memory
- Medium usage: 512 CPU, 1024 MB memory (default)
- Heavy usage: 1024 CPU, 2048 MB memory

### Dependency Groups

To include specific dependency groups (ROS1, ROS2, PX4, etc.), modify the Docker build:

```bash
docker build \
  -f docker/Dockerfile.ecs \
  --build-arg DEPENDENCY_GROUPS="ros2 px4" \
  -t bagel-mcp-server:latest \
  .
```

## Connecting Your Project

### MCP Client Connection

Once deployed, connect to the MCP server from your project:

```python
# Example: Using MCP client
from mcp import ClientSession, StdioServerParameters
import httpx

# For HTTP/SSE transport
async with httpx.AsyncClient() as client:
    response = await client.get(
        "http://YOUR_ECS_ENDPOINT:8000/sse",
        headers={"Accept": "text/event-stream"}
    )
    # Handle SSE stream
```

### Endpoint URL

The endpoint URL will be:
- **With Load Balancer**: `http://your-alb-dns-name:8000`
- **Direct ECS**: `http://<task-ip>:8000` (not recommended for production)

### Health Check

The service includes a health check endpoint:
```bash
curl http://YOUR_ENDPOINT:8000/health
```

## Monitoring

### CloudWatch Logs

Logs are automatically sent to CloudWatch:
- **Log Group**: `/ecs/bagel-mcp-server`
- **Log Stream**: `ecs/bagel-mcp-server/<task-id>`

View logs:
```bash
aws logs tail /ecs/bagel-mcp-server --follow
```

### ECS Service Metrics

Monitor service health:
```bash
aws ecs describe-services \
  --cluster bagel-cluster \
  --services bagel-mcp-service
```

## Scaling

### Manual Scaling

```bash
aws ecs update-service \
  --cluster bagel-cluster \
  --service bagel-mcp-service \
  --desired-count 2
```

### Auto Scaling

Create an auto-scaling target:

```bash
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --scalable-dimension ecs:service:DesiredCount \
  --resource-id service/bagel-cluster/bagel-mcp-service \
  --min-capacity 1 \
  --max-capacity 10
```

## Troubleshooting

### Container Not Starting

1. Check CloudWatch logs
2. Verify task definition and image URI
3. Check security group allows inbound on port 8000
4. Verify IAM roles have correct permissions

### Connection Issues

1. Verify security group allows traffic from your source
2. Check if service is running: `aws ecs describe-services`
3. Test health endpoint: `curl http://endpoint:8000/health`

### High Memory Usage

1. Increase memory allocation in task definition
2. Review cache settings (`BAGEL_USE_CACHE`)
3. Monitor CloudWatch metrics

## Security Best Practices

1. **Use Application Load Balancer** with SSL/TLS termination
2. **Restrict Security Groups** to only necessary sources
3. **Use IAM Roles** for task execution (not access keys)
4. **Enable VPC Flow Logs** for network monitoring
5. **Use Secrets Manager** for sensitive configuration

## Cost Optimization

- Use **Fargate Spot** for non-critical workloads (up to 70% savings)
- Right-size CPU and memory based on actual usage
- Enable **ECS Service Auto Scaling** based on metrics
- Use **CloudWatch Insights** to optimize resource usage

## Next Steps

1. Set up Application Load Balancer for production
2. Configure auto-scaling policies
3. Set up CloudWatch alarms
4. Configure backup for persistent data (if needed)
5. Set up CI/CD pipeline for automated deployments
