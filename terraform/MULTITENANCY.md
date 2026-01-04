# Multitenancy Guide for Bagel MCP Server

## Overview

This guide explains how to handle multitenancy (multiple customers/users) with Bagel MCP Server on ECS.

## Multitenancy Models

### Model 1: Shared Service with S3 Prefix Isolation (Recommended)

**Best for**: Cost efficiency, shared resources, moderate isolation needs

- **Single ECS service** handles all tenants
- **S3 bucket prefixes** separate tenant data: `s3://bucket/tenant-1/`, `s3://bucket/tenant-2/`
- **IAM policies** restrict access to tenant-specific prefixes
- **Application-level** tenant identification (API keys, headers, etc.)

**Pros:**
- Lower cost (shared infrastructure)
- Easier to manage
- Good for most use cases

**Cons:**
- Less isolation between tenants
- Requires application-level tenant routing

**Setup:**
```hcl
# In terraform.tfvars
enable_multitenancy = true
tenant_names = ["tenant-1", "tenant-2", "tenant-3"]
```

### Model 2: Separate ECS Services per Tenant (Maximum Isolation)

**Best for**: High security requirements, compliance needs, complete isolation

- **Separate ECS service** for each tenant
- **Separate S3 buckets** or prefixes per tenant
- **Separate IAM roles** per tenant
- **Complete resource isolation**

**Pros:**
- Maximum isolation
- Easier compliance
- Independent scaling per tenant

**Cons:**
- Higher cost
- More complex management
- More resources to manage

**Setup:**
Uncomment the `aws_ecs_service.bagel_mcp_tenant` resource in `multitenancy.tf`

### Model 3: Namespace-based (Application-Level)

**Best for**: Simple use cases, internal multi-user systems

- **Single service** with application-level namespaces
- **Tenant identification** via API keys, headers, or authentication
- **Application logic** handles tenant isolation
- **Shared S3 bucket** with prefix-based separation

**Pros:**
- Simplest to implement
- Lowest cost
- Flexible

**Cons:**
- Requires application changes
- Less isolation

## Implementation Details

### S3 Storage Structure

```
s3://bagel-prod-storage-{account-id}/
├── tenants/
│   ├── tenant-1/
│   │   ├── robologs/
│   │   ├── artifacts/
│   │   └── datasets/
│   ├── tenant-2/
│   │   ├── robologs/
│   │   ├── artifacts/
│   │   └── datasets/
│   └── shared/  # Optional shared resources
└── cache/       # Shared cache
```

### Tenant Identification

#### Option A: API Key in Header
```python
# Client sends:
headers = {
    "X-Tenant-ID": "tenant-1",
    "Authorization": "Bearer <api-key>"
}
```

#### Option B: Subdomain Routing
```
tenant-1.bagel.example.com  → Routes to tenant-1
tenant-2.bagel.example.com  → Routes to tenant-2
```

#### Option C: Path-based
```
https://bagel.example.com/tenant-1/...
https://bagel.example.com/tenant-2/...
```

### IAM Policies

With multitenancy enabled, IAM policies automatically restrict access:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject"],
  "Resource": "arn:aws:s3:::bucket/tenants/tenant-1/*"
}
```

## Configuration Examples

### Example 1: Enable Multitenancy with 3 Tenants

```hcl
# terraform.tfvars
enable_multitenancy = true
tenant_names = [
  "acme-corp",
  "widget-inc",
  "tech-startup"
]
```

### Example 2: Single Tenant (Default)

```hcl
# terraform.tfvars
enable_multitenancy = false
# tenant_names not needed
```

### Example 3: Custom Tenant Configuration

```hcl
# terraform.tfvars
enable_multitenancy = true
tenant_names = ["tenant-1", "tenant-2"]

# Each tenant can have custom resources
# (requires modifying multitenancy.tf)
```

## Application Changes Needed

To support multitenancy in your Bagel application:

### 1. Extract Tenant ID

```python
# In server.py or middleware
def get_tenant_id(request):
    # From header
    tenant_id = request.headers.get("X-Tenant-ID")
    
    # Or from subdomain
    # tenant_id = request.host.split(".")[0]
    
    # Or from path
    # tenant_id = request.path.split("/")[1]
    
    return tenant_id
```

### 2. Use Tenant-Specific S3 Paths

```python
import os
from settings import settings

def get_tenant_storage_path(tenant_id, filename):
    s3_bucket = os.getenv("BAGEL_S3_BUCKET")
    return f"s3://{s3_bucket}/tenants/{tenant_id}/{filename}"
```

### 3. Update Environment Variables

```hcl
# In terraform/ecs.tf, add to container environment:
environment = [
  {
    name  = "BAGEL_S3_BUCKET"
    value = aws_s3_bucket.bagel_storage.bucket
  },
  {
    name  = "BAGEL_MULTITENANCY_ENABLED"
    value = tostring(var.enable_multitenancy)
  }
]
```

## Security Considerations

### 1. Tenant Isolation
- ✅ S3 prefix-based isolation (prevents cross-tenant access)
- ✅ IAM policies enforce boundaries
- ✅ Consider separate VPCs for high-security tenants

### 2. Authentication
- Implement API key validation
- Use AWS Cognito or similar for user authentication
- Consider OAuth2/JWT tokens

### 3. Rate Limiting
- Per-tenant rate limits
- Use API Gateway or ALB rules
- Monitor usage per tenant

### 4. Monitoring
- CloudWatch metrics per tenant
- Separate log groups per tenant (optional)
- Cost tracking per tenant

## Cost Optimization

### Shared Service Model
- **Cost**: ~$50-200/month base + usage
- **Scales**: Automatically with load
- **Best for**: 1-100 tenants

### Separate Services Model
- **Cost**: ~$50-200/month per tenant
- **Scales**: Per tenant
- **Best for**: <10 tenants with high isolation needs

## Migration Path

1. **Start with single tenant** (default)
2. **Enable multitenancy** when needed
3. **Add tenants** to `tenant_names` list
4. **Apply Terraform** to update IAM policies
5. **Update application** to handle tenant routing

## Troubleshooting

### Tenant Can't Access S3
- Check IAM policy for tenant prefix
- Verify tenant ID matches configured name
- Check S3 bucket permissions

### Cross-Tenant Data Access
- Review IAM policies
- Check application tenant routing logic
- Enable CloudTrail for audit

### High Costs
- Review tenant usage
- Consider shared service model
- Implement per-tenant quotas
