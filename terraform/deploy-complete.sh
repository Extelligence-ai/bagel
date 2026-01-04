#!/bin/bash
# Complete deployment script: Infrastructure + Docker Image + Service Start

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Complete Bagel MCP Server Deployment${NC}"
echo "=========================================="
echo ""

# Step 1: Deploy Infrastructure
echo -e "${YELLOW}Step 1: Deploying Infrastructure...${NC}"
terraform init
terraform plan -out=tfplan

read -p "Apply infrastructure changes? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    terraform apply tfplan
    rm -f tfplan
    echo -e "${GREEN}✅ Infrastructure deployed${NC}"
else
    echo "Cancelled."
    rm -f tfplan
    exit 1
fi

# Step 2: Build and Push Docker Image
echo ""
echo -e "${YELLOW}Step 2: Building and Pushing Docker Image...${NC}"

ECR_URL=$(terraform output -raw ecr_repository_url)
REGION=$(grep '^aws_region' terraform.tfvars | head -1 | sed 's/.*= *"\(.*\)".*/\1/' | sed 's/.*= *\(.*\)/\1/' || echo "us-east-1")

echo "ECR Repository: $ECR_URL"
echo ""

# Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin $ECR_URL

# Build image
echo "Building Docker image..."
docker build \
  -f ../docker/Dockerfile.ecs \
  --build-arg DEV_MODE=false \
  --build-arg BAGEL_LOCAL_HOST=0.0.0.0 \
  --build-arg BAGEL_MCP_LOCAL_PORT=8000 \
  -t $ECR_URL:latest \
  ..

# Push image
echo "Pushing image to ECR..."
docker push $ECR_URL:latest
echo -e "${GREEN}✅ Image pushed${NC}"

# Step 3: Start ECS Service
echo ""
echo -e "${YELLOW}Step 3: Starting ECS Service...${NC}"

CLUSTER=$(terraform output -raw ecs_cluster_name)
SERVICE=$(terraform output -raw ecs_service_name)

echo "Updating ECS service..."
aws ecs update-service \
  --cluster $CLUSTER \
  --service $SERVICE \
  --force-new-deployment \
  --region $REGION > /dev/null

echo "Waiting for service to stabilize..."
aws ecs wait services-stable \
  --cluster $CLUSTER \
  --services $SERVICE \
  --region $REGION

echo -e "${GREEN}✅ Service is running${NC}"

# Step 4: Get Service URL
echo ""
echo -e "${GREEN}🎉 Deployment Complete!${NC}"
echo "================================"
echo ""

# Check if load balancer is enabled
ALB_DNS=$(terraform output -raw alb_dns_name 2>/dev/null || echo "")

if [ -n "$ALB_DNS" ] && [ "$ALB_DNS" != "null" ]; then
    echo -e "${GREEN}📡 Your MCP Server URL:${NC}"
    echo "   http://$ALB_DNS:8000"
    echo ""
    echo "✅ Use this URL in your other project!"
else
    echo -e "${YELLOW}⚠️  No Load Balancer configured${NC}"
    echo ""
    echo "Getting ECS task IP (may change on restart)..."
    
    # Get task IP
    TASK_ARN=$(aws ecs list-tasks \
      --cluster $CLUSTER \
      --service-name $SERVICE \
      --region $REGION \
      --query 'taskArns[0]' \
      --output text 2>/dev/null || echo "")
    
    if [ -n "$TASK_ARN" ] && [ "$TASK_ARN" != "None" ]; then
        ENI_ID=$(aws ecs describe-tasks \
          --cluster $CLUSTER \
          --tasks $TASK_ARN \
          --region $REGION \
          --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
          --output text 2>/dev/null || echo "")
        
        if [ -n "$ENI_ID" ]; then
            TASK_IP=$(aws ec2 describe-network-interfaces \
              --network-interface-ids $ENI_ID \
              --region $REGION \
              --query 'NetworkInterfaces[0].Association.PublicIp' \
              --output text 2>/dev/null || echo "")
            
            if [ -n "$TASK_IP" ] && [ "$TASK_IP" != "None" ]; then
                echo -e "${GREEN}📡 Your MCP Server URL:${NC}"
                echo "   http://$TASK_IP:8000"
                echo ""
                echo -e "${YELLOW}⚠️  Note: This IP may change when tasks restart${NC}"
                echo "   Consider enabling load balancer for stable URL"
            fi
        fi
    fi
    
    echo ""
    echo -e "${YELLOW}💡 To get a stable URL, enable load balancer:${NC}"
    echo "   1. Set enable_load_balancer = true in terraform.tfvars"
    echo "   2. Run: terraform apply"
fi

echo ""
echo -e "${BLUE}📋 Service Information:${NC}"
echo "   Cluster: $CLUSTER"
echo "   Service: $SERVICE"
echo "   Log Group: $(terraform output -raw cloudwatch_log_group)"
echo ""
echo -e "${BLUE}🔗 View Logs:${NC}"
echo "   aws logs tail $(terraform output -raw cloudwatch_log_group) --follow"
echo ""
echo -e "${BLUE}✅ Ready to connect from your other project!${NC}"
