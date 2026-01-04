#!/bin/bash
# Script to create ECS service for Bagel MCP Server

set -e

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
ECS_CLUSTER_NAME="${ECS_CLUSTER_NAME:-bagel-cluster}"
ECS_SERVICE_NAME="${ECS_SERVICE_NAME:-bagel-mcp-service}"
TASK_DEFINITION_FAMILY="${TASK_DEFINITION_FAMILY:-bagel-mcp-server}"
SUBNET_IDS="${SUBNET_IDS:-subnet-12345,subnet-67890}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-sg-12345}"
DESIRED_COUNT="${DESIRED_COUNT:-1}"

echo "🚀 Creating ECS Service for Bagel MCP Server"
echo "=========================================="

# Get the latest task definition
LATEST_TASK_DEF=$(aws ecs describe-task-definition \
  --task-definition ${TASK_DEFINITION_FAMILY} \
  --region ${AWS_REGION} \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text)

echo "Using task definition: ${LATEST_TASK_DEF}"

# Create the service
aws ecs create-service \
  --cluster ${ECS_CLUSTER_NAME} \
  --service-name ${ECS_SERVICE_NAME} \
  --task-definition ${LATEST_TASK_DEF} \
  --desired-count ${DESIRED_COUNT} \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNET_IDS}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
  --region ${AWS_REGION}

echo "✅ Service created: ${ECS_SERVICE_NAME}"
