# Billing and Usage Tracking Guide

## Overview

This guide explains how to track and bill tenant usage in the shared service multitenancy model (Model 1).

## How It Works

### 1. Cost Allocation Tags

All resources are automatically tagged with tenant information:

- **Tenant**: Tenant identifier
- **CostCenter**: `bagel-{tenant-name}`
- **BillingGroup**: `bagel-multitenant`

These tags are automatically picked up by **AWS Cost Explorer** for billing.

### 2. S3 Usage Tracking

Each tenant's data is stored in separate S3 prefixes:
```
s3://bucket/tenants/tenant-1/robologs/
s3://bucket/tenants/tenant-2/robologs/
```

S3 Inventory is configured to track usage per prefix daily.

### 3. CloudWatch Metrics

CloudWatch metrics track:
- S3 storage per tenant
- ECS task usage (shared, but can be estimated per tenant)
- API request counts (if instrumented)

## Viewing Costs in AWS Console

### Method 1: AWS Cost Explorer

1. Go to **AWS Cost Explorer**
2. Click **Costs by resource**
3. Filter by tags:
   - **Tag Key**: `Tenant`
   - **Tag Value**: `tenant-1` (or your tenant name)
4. View costs for that tenant

### Method 2: Cost Allocation Tags Report

1. Go to **Billing Dashboard**
2. Click **Cost Allocation Tags**
3. Enable tags: `Tenant`, `CostCenter`, `BillingGroup`
4. Wait 24 hours for data to populate
5. View reports filtered by tenant

### Method 3: Using the Script

Use the provided Python script to get detailed usage:

```bash
# Install dependencies
pip install boto3

# Get usage for a specific tenant
python terraform/scripts/get-tenant-usage.py \
  --tenant tenant-1 \
  --bucket bagel-prod-storage-495599747090 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# Get usage for all tenants
python terraform/scripts/get-tenant-usage.py \
  --all-tenants \
  --bucket bagel-prod-storage-495599747090 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --output csv
```

## Cost Breakdown

### S3 Storage Costs

**Per Tenant:**
- Standard Storage: $0.023 per GB/month
- Standard-IA: $0.0125 per GB/month (after 30 days)
- Glacier: $0.004 per GB/month (after 90 days)

**Tracking:**
- S3 Inventory reports daily
- CloudWatch metrics track bucket size per prefix
- Script calculates exact usage

### ECS Compute Costs

**Shared Infrastructure:**
- Fargate: $0.04048 per vCPU-hour + $0.004445 per GB-hour
- Base cost: ~$15-20/month for 0.5 vCPU, 1GB

**Per-Tenant Allocation:**
Since ECS is shared, you can allocate costs by:
1. **Usage-based**: Track API calls per tenant
2. **Storage-based**: Proportion to S3 usage
3. **Fixed allocation**: Equal split across tenants

### Data Transfer Costs

- Outbound: $0.09 per GB
- Track via CloudWatch metrics per tenant

## Setting Up Billing Reports

### Step 1: Enable Cost Allocation Tags

```bash
# Tags are automatically applied by Terraform
# Just verify they exist:
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=Tenant,Values=tenant-1
```

### Step 2: Create Cost Explorer Report

1. Go to **Cost Explorer** → **Reports**
2. Create custom report:
   - **Group by**: Tag `Tenant`
   - **Time period**: Monthly
   - **Service**: ECS, S3
3. Save as "Tenant Billing Report"

### Step 3: Set Up Billing Alerts

```bash
# Create budget per tenant
aws budgets create-budget \
  --account-id $(terraform output -raw aws_account_id) \
  --budget file://budget-template.json
```

## Monthly Billing Workflow

### 1. Generate Usage Report

```bash
# At end of month
python terraform/scripts/get-tenant-usage.py \
  --all-tenants \
  --bucket $(terraform output -raw s3_bucket_name) \
  --cluster $(terraform output -raw ecs_cluster_name) \
  --service $(terraform output -raw ecs_service_name) \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --output csv > monthly-billing-2024-01.csv
```

### 2. Verify in Cost Explorer

1. Open AWS Cost Explorer
2. Filter by month and tenant tags
3. Compare with script output

### 3. Generate Invoice

Use the CSV output to create invoices for each tenant.

## Advanced: Per-Tenant API Tracking

To track API usage per tenant, add custom metrics in your application:

```python
import boto3

cloudwatch = boto3.client('cloudwatch')

def track_tenant_usage(tenant_id, operation, duration_ms):
    cloudwatch.put_metric_data(
        Namespace='Bagel/TenantUsage',
        MetricData=[
            {
                'MetricName': 'APICalls',
                'Dimensions': [
                    {'Name': 'Tenant', 'Value': tenant_id},
                    {'Name': 'Operation', 'Value': operation}
                ],
                'Value': 1,
                'Unit': 'Count'
            },
            {
                'MetricName': 'APILatency',
                'Dimensions': [
                    {'Name': 'Tenant', 'Value': tenant_id},
                    {'Name': 'Operation', 'Value': operation}
                ],
                'Value': duration_ms,
                'Unit': 'Milliseconds'
            }
        ]
    )
```

Then query in CloudWatch:

```bash
aws cloudwatch get-metric-statistics \
  --namespace Bagel/TenantUsage \
  --metric-name APICalls \
  --dimensions Name=Tenant,Value=tenant-1 \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-31T23:59:59Z \
  --period 86400 \
  --statistics Sum
```

## Cost Optimization Tips

### 1. Right-Size Per Tenant

Monitor usage and adjust:
- S3 lifecycle policies per tenant
- ECS task size if tenants have different needs

### 2. Implement Quotas

```python
# Example: Limit storage per tenant
def check_tenant_quota(tenant_id, new_size_bytes):
    current_usage = get_tenant_s3_usage(tenant_id)
    quota = get_tenant_quota(tenant_id)  # e.g., 100 GB
    
    if current_usage + new_size_bytes > quota:
        raise QuotaExceededError(f"Tenant {tenant_id} exceeded quota")
```

### 3. Charge for Premium Features

Track feature usage:
- Trajectory analysis calls
- Large file processing
- Real-time processing

## Example Billing Output

```
Tenant Usage Report: 2024-01-01 to 2024-01-31
================================================================================

Tenant: acme-corp
  S3 Storage: 45.23 GB
  S3 Objects: 1,234
  Estimated Costs:
    S3 Storage: $1.04
    S3 Requests: $0.05
    ECS (estimated): $2.50
    Total: $3.59

Tenant: widget-inc
  S3 Storage: 12.45 GB
  S3 Objects: 456
  Estimated Costs:
    S3 Storage: $0.29
    S3 Requests: $0.02
    ECS (estimated): $0.75
    Total: $1.06
```

## Troubleshooting

### No Cost Data Showing

1. **Wait 24-48 hours** after enabling tags
2. Verify tags are applied: `aws resourcegroupstaggingapi get-resources`
3. Check Cost Allocation Tags are enabled in Billing Dashboard

### Inaccurate Costs

1. Verify S3 Inventory is running: Check `s3://bucket/inventory/`
2. Check CloudWatch metrics are being published
3. Review script calculations match AWS pricing

### Missing Tenant Data

1. Verify tenant prefix exists in S3
2. Check IAM permissions for CloudWatch metrics
3. Ensure tenant name matches exactly (case-sensitive)

## Next Steps

1. **Enable multitenancy** in `terraform.tfvars`
2. **Apply Terraform** to create billing resources
3. **Wait 24 hours** for cost data to populate
4. **Set up Cost Explorer** reports
5. **Run monthly billing script** to generate invoices
