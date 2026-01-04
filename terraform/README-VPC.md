# Understanding VPCs and Subnets for ECS Deployment

## What are VPCs and Subnets?

### VPC (Virtual Private Cloud)
- **Think of it as**: Your own private network in AWS
- **Like**: A virtual data center that's isolated from other AWS customers
- **Why you need it**: ECS tasks need to run in a network (VPC)

### Subnet
- **Think of it as**: A section of your VPC in a specific data center location
- **Like**: A room in your data center
- **Why you need it**: ECS tasks run in subnets, and you need at least 2 in different locations for reliability

## Quick Start: Use Your Default VPC

Most AWS accounts come with a **default VPC** that you can use immediately!

### Option 1: Find Your Default VPC (Easiest)

Run this script to automatically find your VPC and subnets:

```bash
cd terraform
./find-vpc-info.sh
```

It will show you:
- Your default VPC ID
- Available subnets
- Ready-to-use configuration for `terraform.tfvars`

### Option 2: Find Manually

1. **Find your VPC ID:**
```bash
aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text
```

2. **Find subnets in that VPC:**
```bash
# Replace vpc-xxxxx with your VPC ID from step 1
aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-xxxxx" --query 'Subnets[*].SubnetId' --output text
```

3. **Add to terraform.tfvars:**
```hcl
vpc_id = "vpc-0123456789abcdef0"  # Your VPC ID from step 1
subnet_ids = [
  "subnet-0123456789abcdef0",      # First subnet
  "subnet-abcdef0123456789"        # Second subnet (different AZ)
]
```

## Understanding the Values

### VPC ID Format
- Always starts with `vpc-`
- Example: `vpc-0123456789abcdef0`

### Subnet ID Format
- Always starts with `subnet-`
- Example: `subnet-0123456789abcdef0`

### Why Multiple Subnets?
- **High Availability**: If one data center has issues, your service keeps running
- **ECS Requirement**: Fargate needs subnets in different Availability Zones
- **Best Practice**: Use at least 2 subnets in different AZs

## Common Scenarios

### Scenario 1: New AWS Account
✅ **Use Default VPC** - It's already set up and ready to use!

### Scenario 2: Existing AWS Account with Custom VPC
✅ **Use Your Custom VPC** - If you already have infrastructure, use that VPC

### Scenario 3: No VPC at All
❌ **Create One First** - You need a VPC before deploying ECS

## Step-by-Step: First Time Setup

### 1. Check if you have a default VPC

```bash
aws ec2 describe-vpcs --filters "Name=isDefault,Values=true"
```

If you see output, you have a default VPC! ✅

### 2. Get your VPC ID

```bash
aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text
```

Copy the output (e.g., `vpc-0123456789abcdef0`)

### 3. Get subnets in that VPC

```bash
# Replace with your VPC ID
VPC_ID="vpc-0123456789abcdef0"
aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[*].[SubnetId,AvailabilityZone]' --output table
```

### 4. Pick at least 2 subnets from different Availability Zones

Look for subnets in different AZs (e.g., `us-east-1a` and `us-east-1b`)

### 5. Add to terraform.tfvars

```hcl
vpc_id = "vpc-0123456789abcdef0"
subnet_ids = [
  "subnet-0123456789abcdef0",  # From us-east-1a
  "subnet-abcdef0123456789"    # From us-east-1b
]
```

## Visual Example

```
AWS Account
└── VPC (vpc-xxxxx) - Your private network
    ├── Subnet 1 (subnet-aaaaa) - Data center in us-east-1a
    ├── Subnet 2 (subnet-bbbbb) - Data center in us-east-1b
    └── Subnet 3 (subnet-ccccc) - Data center in us-east-1c
    
ECS Service runs in:
    ├── Subnet 1 (subnet-aaaaa) ✅
    └── Subnet 2 (subnet-bbbbb) ✅
```

## Troubleshooting

### "No default VPC found"
- Your account might not have a default VPC
- Create one: `aws ec2 create-default-vpc`
- Or use an existing VPC

### "Not enough subnets"
- You need at least 2 subnets in different Availability Zones
- Check: `aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-xxxxx"`

### "Subnets in same AZ"
- ECS Fargate requires subnets in different AZs
- Pick subnets from different zones (e.g., us-east-1a and us-east-1b)

## Still Confused?

**Simplest approach**: Run the helper script!

```bash
cd terraform
./find-vpc-info.sh
```

It will give you everything you need! 🚀
