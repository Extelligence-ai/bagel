#!/bin/bash
# Deployment script for Bagel MCP Server to ECS

set -e

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-YOUR_ACCOUNT_ID}"
ECR_REPO_NAME="${ECR_REPO_NAME:-bagel-mcp-server}"
ECS_CLUSTER_NAME="${ECS_CLUSTER_NAME:-bagel-cluster}"
ECS_SERVICE_NAME="${ECS_SERVICE_NAME:-bagel-mcp-service}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying Bagel MCP Server to ECS${NC}"
echo "=========================================="

# Step 1: Build Docker image
echo -e "\n${YELLOW}Step 1: Building Docker image...${NC}"
docker build \
  -f docker/Dockerfile.ecs \
  --build-arg DEV_MODE=false \
  --build-arg DEPENDENCY_GROUPS="" \
  --build-arg BAGEL_LOCAL_HOST=0.0.0.0 \
  --build-arg BAGEL_MCP_LOCAL_PORT=8000 \
  -t ${ECR_REPO_NAME}:${IMAGE_TAG} \
  -t ${ECR_REPO_NAME}:latest \
  .

# Step 2: Login to ECR
echo -e "\n${YELLOW}Step 2: Logging into ECR...${NC}"
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin \
  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Step 3: Create ECR repository if it doesn't exist
echo -e "\n${YELLOW}Step 3: Ensuring ECR repository exists...${NC}"
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"
if ! aws ecr describe-repositories --repository-names ${ECR_REPO_NAME} --region ${AWS_REGION} &>/dev/null; then
  echo "Creating ECR repository: ${ECR_REPO_NAME}"
  aws ecr create-repository \
    --repository-name ${ECR_REPO_NAME} \
    --region ${AWS_REGION} \
    --image-scanning-configuration scanOnPush=true
else
  echo "ECR repository already exists: ${ECR_REPO_NAME}"
fi

# Step 4: Tag and push image
echo -e "\n${YELLOW}Step 4: Tagging and pushing image to ECR...${NC}"
docker tag ${ECR_REPO_NAME}:${IMAGE_TAG} ${ECR_REPO_URI}:${IMAGE_TAG}
docker tag ${ECR_REPO_NAME}:${IMAGE_TAG} ${ECR_REPO_URI}:latest
docker push ${ECR_REPO_URI}:${IMAGE_TAG}
docker push ${ECR_REPO_URI}:latest

# Step 5: Update task definition
echo -e "\n${YELLOW}Step 5: Updating ECS task definition...${NC}"
# Replace placeholders in task definition
sed "s|YOUR_ECR_REPO_URI|${ECR_REPO_URI}|g; s|YOUR_ACCOUNT_ID|${AWS_ACCOUNT_ID}|g" \
  ecs/task-definition.json > ecs/task-definition-updated.json

# Register new task definition
TASK_DEF_ARN=$(aws ecs register-task-definition \
  --cli-input-json file://ecs/task-definition-updated.json \
  --region ${AWS_REGION} \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text)

echo "Task definition registered: ${TASK_DEF_ARN}"

# Step 6: Update ECS service (if it exists)
echo -e "\n${YELLOW}Step 6: Updating ECS service...${NC}"
if aws ecs describe-services \
  --cluster ${ECS_CLUSTER_NAME} \
  --services ${ECS_SERVICE_NAME} \
  --region ${AWS_REGION} &>/dev/null; then
  echo "Updating existing service: ${ECS_SERVICE_NAME}"
  aws ecs update-service \
    --cluster ${ECS_CLUSTER_NAME} \
    --service ${ECS_SERVICE_NAME} \
    --task-definition ${TASK_DEF_ARN} \
    --force-new-deployment \
    --region ${AWS_REGION} > /dev/null
  echo -e "${GREEN}✅ Service update initiated${NC}"
else
  echo -e "${YELLOW}⚠️  Service ${ECS_SERVICE_NAME} does not exist. Create it manually or use the create-service.sh script.${NC}"
fi

# Cleanup
rm -f ecs/task-definition-updated.json

echo -e "\n${GREEN}✅ Deployment complete!${NC}"
echo "Task Definition ARN: ${TASK_DEF_ARN}"
echo "ECR Image URI: ${ECR_REPO_URI}:${IMAGE_TAG}"
