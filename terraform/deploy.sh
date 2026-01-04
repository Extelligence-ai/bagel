#!/bin/bash
# Complete deployment script using Terraform

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}🚀 Deploying Bagel MCP Server with Terraform${NC}"
echo "=========================================="

# Check if terraform.tfvars exists
if [ ! -f "terraform.tfvars" ]; then
    echo -e "${YELLOW}⚠️  terraform.tfvars not found${NC}"
    echo "Copying example file..."
    cp terraform.tfvars.example terraform.tfvars
    echo -e "${RED}Please edit terraform.tfvars with your values before continuing!${NC}"
    exit 1
fi

# Step 1: Initialize Terraform
echo -e "\n${YELLOW}Step 1: Initializing Terraform...${NC}"
terraform init

# Step 2: Plan
echo -e "\n${YELLOW}Step 2: Planning infrastructure changes...${NC}"
terraform plan -out=tfplan

# Step 3: Apply
echo -e "\n${YELLOW}Step 3: Applying infrastructure changes...${NC}"
read -p "Do you want to apply these changes? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    terraform apply tfplan
    rm -f tfplan
else
    echo "Cancelled."
    rm -f tfplan
    exit 1
fi

# Step 4: Get ECR URL
echo -e "\n${YELLOW}Step 4: Getting ECR repository URL...${NC}"
ECR_URL=$(terraform output -raw ecr_repository_url)
echo "ECR URL: $ECR_URL"

# Step 5: Build and push Docker image
echo -e "\n${YELLOW}Step 5: Building and pushing Docker image...${NC}"
read -p "Do you want to build and push the Docker image? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Login to ECR
    AWS_REGION=${AWS_REGION:-us-east-1}
    aws ecr get-login-password --region $AWS_REGION | \
      docker login --username AWS --password-stdin $ECR_URL

    # Build image
    docker build \
      -f ../docker/Dockerfile.ecs \
      --build-arg DEV_MODE=false \
      --build-arg BAGEL_LOCAL_HOST=0.0.0.0 \
      --build-arg BAGEL_MCP_LOCAL_PORT=8000 \
      -t $ECR_URL:latest \
      ..

    # Push image
    docker push $ECR_URL:latest
    echo -e "${GREEN}✅ Image pushed successfully${NC}"
fi

# Step 6: Update ECS service
echo -e "\n${YELLOW}Step 6: Updating ECS service...${NC}"
CLUSTER_NAME=$(terraform output -raw ecs_cluster_name)
SERVICE_NAME=$(terraform output -raw ecs_service_name)

aws ecs update-service \
  --cluster $CLUSTER_NAME \
  --service $SERVICE_NAME \
  --force-new-deployment \
  --region $AWS_REGION > /dev/null

echo -e "${GREEN}✅ Service update initiated${NC}"

# Step 7: Show outputs
echo -e "\n${GREEN}✅ Deployment complete!${NC}"
echo -e "\n📋 Outputs:"
terraform output

echo -e "\n🔗 Service Endpoint:"
terraform output service_endpoint
