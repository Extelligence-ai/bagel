#!/bin/bash
# Script to help find your VPC and subnet IDs

echo "🔍 Finding your AWS VPC and Subnet Information"
echo "=============================================="
echo ""

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    echo "❌ AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

echo "📋 Your AWS Account Information:"
aws sts get-caller-identity
echo ""

# Get default VPC
echo "🔍 Looking for Default VPC..."
DEFAULT_VPC=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text 2>/dev/null)

if [ "$DEFAULT_VPC" != "None" ] && [ -n "$DEFAULT_VPC" ]; then
    echo "✅ Found Default VPC: $DEFAULT_VPC"
    echo ""
    
    # Get subnets in default VPC
    echo "🔍 Finding Subnets in Default VPC..."
    SUBNETS=$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$DEFAULT_VPC" \
        --query 'Subnets[*].[SubnetId,AvailabilityZone]' \
        --output text)
    
    if [ -n "$SUBNETS" ]; then
        echo "✅ Found Subnets:"
        echo "$SUBNETS" | while read subnet_id az; do
            echo "   - $subnet_id (AZ: $az)"
        done
        echo ""
        
        # Create terraform.tfvars snippet
        SUBNET_IDS=$(echo "$SUBNETS" | awk '{print $1}' | tr '\n' ',' | sed 's/,$//' | sed 's/,/", "/g')
        echo "📝 Add this to your terraform.tfvars:"
        echo ""
        echo "vpc_id = \"$DEFAULT_VPC\""
        echo "subnet_ids = [\"$SUBNET_IDS\"]"
        echo ""
    else
        echo "⚠️  No subnets found in default VPC"
    fi
else
    echo "⚠️  No default VPC found"
    echo ""
    echo "🔍 Listing all VPCs..."
    aws ec2 describe-vpcs \
        --query 'Vpcs[*].[VpcId,CidrBlock,IsDefault,Tags[?Key==`Name`].Value|[0]]' \
        --output table
    
    echo ""
    echo "💡 If you see VPCs above, you can use any of them."
    echo "   To find subnets for a specific VPC, run:"
    echo "   aws ec2 describe-subnets --filters \"Name=vpc-id,Values=vpc-xxxxx\""
fi

echo ""
echo "📚 What are VPCs and Subnets?"
echo "=============================="
echo "VPC (Virtual Private Cloud): Your private network in AWS"
echo "Subnet: A section of your VPC in a specific data center (Availability Zone)"
echo ""
echo "For ECS Fargate, you need:"
echo "  - A VPC (your network)"
echo "  - At least 2 subnets in different Availability Zones (for high availability)"
echo ""
