#!/bin/bash
# Complete deployment: Build, Push, Deploy, Get URL

set -e

echo "🚀 Deploying Bagel MCP Server to ECS"
echo "===================================="
echo ""

# Get ECR URL and region
cd "$(dirname "$0")"
ECR_URL=$(terraform output -raw ecr_repository_url 2>/dev/null || echo "")
REGION=$(terraform output -raw aws_region 2>/dev/null || echo "us-east-1")

if [ -z "$ECR_URL" ]; then
    echo "❌ Error: ECR repository not found. Run 'terraform apply' first."
    exit 1
fi

echo "📦 Step 1: Building Docker image..."
cd ..
docker build -f docker/Dockerfile.ecs \
  --build-arg DEV_MODE=false \
  --build-arg BAGEL_LOCAL_HOST=0.0.0.0 \
  --build-arg BAGEL_MCP_LOCAL_PORT=8000 \
  -t $ECR_URL:latest . || {
    echo "❌ Docker build failed"
    exit 1
}

echo ""
echo "📤 Step 2: Pushing to ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin $ECR_URL
docker push $ECR_URL:latest || {
    echo "❌ Docker push failed"
    exit 1
}

echo ""
echo "🔄 Step 3: Updating ECS service..."
cd terraform
CLUSTER=$(terraform output -raw ecs_cluster_name)
SERVICE=$(terraform output -raw ecs_service_name)

aws ecs update-service \
  --cluster $CLUSTER \
  --service $SERVICE \
  --force-new-deployment \
  --region $REGION > /dev/null

echo "⏳ Waiting for service to stabilize (this may take 2-3 minutes)..."
aws ecs wait services-stable \
  --cluster $CLUSTER \
  --services $SERVICE \
  --region $REGION

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📡 Your Bagel MCP Server URL:"
echo "================================"

# Get ALB DNS name if load balancer is enabled
ALB_DNS=$(terraform output -raw alb_dns_name 2>/dev/null || echo "")

if [ -n "$ALB_DNS" ] && [ "$ALB_DNS" != "null" ]; then
    echo "   http://$ALB_DNS:8000"
    echo ""
    echo "✅ This is a stable URL that won't change!"
else
    echo "   ⚠️  No load balancer configured"
    echo "   Getting task IP (may change on restart)..."
    
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
                echo "   http://$TASK_IP:8000"
            fi
        fi
    fi
fi

echo ""
echo "🔗 Use this URL in your other project!"
echo ""
echo "📋 Service Info:"
echo "   Cluster: $CLUSTER"
echo "   Service: $SERVICE"
echo ""
echo "📊 View logs:"
echo "   aws logs tail $(terraform output -raw cloudwatch_log_group) --follow"
