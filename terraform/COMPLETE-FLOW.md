# Complete System Flow: How Everything Works Together

## 🎯 Overview

This document explains how all components work together: Cognito authentication, tenant assignment, S3 storage, and billing.

## 📊 Complete Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Complete System Flow                      │
└─────────────────────────────────────────────────────────────┘

┌──────────────┐
│   Admin      │
│  (You)       │
└──────┬───────┘
       │
       │ 1. Create Tenant
       ▼
┌─────────────────────────────────────┐
│  terraform.tfvars                   │
│  tenant_names = ["acme-corp"]       │
└──────┬──────────────────────────────┘
       │
       │ 2. terraform apply
       ▼
┌─────────────────────────────────────┐
│  AWS Resources Created:             │
│  • S3: bucket/tenants/acme-corp/   │
│  • IAM: Policies for acme-corp      │
│  • CloudWatch: Metrics tags        │
│  • Cognito: User pool ready         │
└──────┬──────────────────────────────┘
       │
       │ 3. Onboard User
       ▼
┌─────────────────────────────────────┐
│  ./onboard-user.sh                  │
│  john@acme.com acme-corp            │
└──────┬──────────────────────────────┘
       │
       │ 4. User Created
       ▼
┌─────────────────────────────────────┐
│  Cognito User:                      │
│  • Email: john@acme.com             │
│  • custom:tenant_id: acme-corp      │
└──────┬──────────────────────────────┘
       │
       │ 5. User Signs In
       ▼
┌─────────────────────────────────────┐
│  Cognito Authentication              │
│  (Email/Password or Google)          │
└──────┬──────────────────────────────┘
       │
       │ 6. JWT Token Issued
       ▼
┌─────────────────────────────────────┐
│  JWT Token:                         │
│  {                                  │
│    "email": "john@acme.com",        │
│    "custom:tenant_id": "acme-corp", │
│    "sub": "user-uuid"               │
│  }                                  │
└──────┬──────────────────────────────┘
       │
       │ 7. User Makes Request
       ▼
┌─────────────────────────────────────┐
│  MCP Server Request:                │
│  POST /tools/analyze_trajectory     │
│  Headers:                           │
│    Authorization: Bearer <token>   │
└──────┬──────────────────────────────┘
       │
       │ 8. Extract Tenant
       ▼
┌─────────────────────────────────────┐
│  Server Logic:                      │
│  1. Verify JWT token                │
│  2. Extract custom:tenant_id        │
│  3. tenant_id = "acme-corp"         │
└──────┬──────────────────────────────┘
       │
       │ 9. Use Tenant for Operations
       ▼
┌─────────────────────────────────────┐
│  S3 Operations:                     │
│  Path: s3://bucket/                 │
│        tenants/acme-corp/           │
│        robologs/race_1.bag          │
│                                      │
│  CloudWatch Metrics:                │
│  Tenant=acme-corp                   │
│  Operation=analyze_trajectory       │
└──────┬──────────────────────────────┘
       │
       │ 10. Track Usage
       ▼
┌─────────────────────────────────────┐
│  Billing System:                    │
│  • S3 storage: 45.23 GB             │
│  • API calls: 1,234                 │
│  • Cost: $3.59                      │
│  • Tagged: Tenant=acme-corp         │
└─────────────────────────────────────┘
```

## 🔄 Step-by-Step: Real Example

### Step 1: Admin Creates Tenant

```bash
# Edit terraform/terraform.tfvars
tenant_names = ["acme-corp", "widget-inc"]

# Apply changes
cd terraform
terraform apply
```

**What happens:**
- ✅ S3 prefixes created: `tenants/acme-corp/`, `tenants/widget-inc/`
- ✅ IAM policies created for each tenant
- ✅ CloudWatch metrics configured
- ✅ Billing tags set up

### Step 2: Admin Onboards User

```bash
# Onboard John from Acme Corp
./terraform/scripts/onboard-user.sh john@acme.com acme-corp

# Onboard Jane from Widget Inc
./terraform/scripts/onboard-user.sh jane@widget.com widget-inc
```

**What happens:**
- ✅ User created in Cognito
- ✅ `custom:tenant_id` attribute set to `acme-corp`
- ✅ Temporary password generated
- ✅ User can now sign in

### Step 3: User Authenticates

**Via Cognito Hosted UI:**
1. User goes to: `https://bagel-auth.auth.us-east-1.amazoncognito.com`
2. Signs in with email/password (or clicks "Sign in with Google")
3. Receives JWT token

**Via Your App:**
```python
import boto3

cognito = boto3.client('cognito-idp')

response = cognito.initiate_auth(
    ClientId=client_id,
    AuthFlow='USER_PASSWORD_AUTH',
    AuthParameters={
        'USERNAME': 'john@acme.com',
        'PASSWORD': 'password123'
    }
)

token = response['AuthenticationResult']['IdToken']
# Token contains: {"custom:tenant_id": "acme-corp", ...}
```

### Step 4: User Makes Request

```python
# User's client code
import requests

response = requests.post(
    "https://your-mcp-server/tools/analyze_trajectory",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    },
    json={
        "robolog_path": "race_1.bag"
    }
)
```

### Step 5: Server Processes Request

```python
# In your MCP server (server.py)
from jose import jwt
import os

def get_tenant_from_token(request):
    """Extract tenant from Cognito JWT token."""
    auth_header = request.headers.get("Authorization")
    token = auth_header.replace("Bearer ", "")
    
    # Decode token (verify signature in production!)
    decoded = jwt.decode(token, options={"verify_signature": False})
    
    # Extract tenant ID
    tenant_id = decoded.get("custom:tenant_id")
    
    if not tenant_id:
        raise HTTPException(403, "No tenant assigned")
    
    return tenant_id

@server.tool(title="Analyze trajectory")
def analyze_trajectory(
    robolog_path: str,
    request: Request,  # Add request to get token
    start_seconds: float | None = None,
    end_seconds: float | None = None
):
    # Get tenant from token
    tenant_id = get_tenant_from_token(request)
    
    # Use tenant-specific S3 path
    s3_bucket = os.getenv("BAGEL_S3_BUCKET")
    s3_path = f"s3://{s3_bucket}/tenants/{tenant_id}/robologs/{robolog_path}"
    
    # Track usage for billing
    track_tenant_usage(tenant_id, "analyze_trajectory", robolog_path)
    
    # Process request...
    # All S3 operations use tenant-specific paths
    # All CloudWatch metrics tagged with tenant
```

### Step 6: Billing Tracks Usage

**Automatically:**
- ✅ S3 storage tracked per tenant prefix
- ✅ CloudWatch metrics tagged with tenant
- ✅ Cost allocation tags applied

**Monthly Billing:**
```bash
# Generate billing report
python terraform/scripts/get-tenant-usage.py \
  --all-tenants \
  --bucket bagel-prod-storage-495599747090 \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --output csv > january-billing.csv
```

**Output:**
```
Tenant,Size_GB,Object_Count,S3_Storage_Cost,Total_Cost
acme-corp,45.23,1234,$1.04,$3.59
widget-inc,12.45,456,$0.29,$1.06
```

## 🔐 Security & Isolation

### Tenant Isolation Guarantees

1. **S3 Path Isolation**
   - Each tenant has separate prefix
   - IAM policies prevent cross-tenant access
   - Users can only access their tenant's data

2. **IAM Policy Example**
   ```json
   {
     "Effect": "Allow",
     "Action": ["s3:GetObject"],
     "Resource": "arn:aws:s3:::bucket/tenants/acme-corp/*"
   }
   ```

3. **Application-Level Checks**
   - Server verifies tenant from token
   - Rejects requests if tenant doesn't match
   - Logs all access attempts

## 📋 Onboarding Checklist

### For Each New Tenant:

- [ ] **1. Add to Terraform**
  ```bash
  # terraform.tfvars
  tenant_names = [..., "new-tenant"]
  terraform apply
  ```

- [ ] **2. Verify Resources Created**
  ```bash
  # Check S3 prefix exists
  aws s3 ls s3://bucket/tenants/new-tenant/
  
  # Check IAM policies
  terraform output
  ```

- [ ] **3. Onboard First User**
  ```bash
  ./onboard-user.sh admin@new-tenant.com new-tenant
  ```

- [ ] **4. Verify Tenant Assignment**
  ```bash
  ./verify-tenant.sh admin@new-tenant.com
  ```

- [ ] **5. Test Authentication**
  - User signs in
  - Receives JWT token
  - Token contains `custom:tenant_id`

- [ ] **6. Test Data Access**
  - User makes API call
  - Server uses tenant-specific S3 path
  - Data is isolated

- [ ] **7. Verify Billing**
  - Check CloudWatch metrics
  - Verify cost allocation tags
  - Test billing script

## 🚨 Common Issues & Solutions

### Issue: User Can't Access Data

**Check:**
1. Tenant assigned? `./verify-tenant.sh user@example.com`
2. Tenant exists in Terraform? `grep tenant_names terraform.tfvars`
3. S3 prefix exists? `aws s3 ls s3://bucket/tenants/tenant-id/`

**Fix:**
```bash
# Re-assign tenant
./onboard-user.sh user@example.com correct-tenant-id
```

### Issue: Wrong Tenant in Token

**Check:**
```bash
# Decode token (online tool or Python)
python -c "import jwt; print(jwt.decode(token, options={'verify_signature': False}))"
```

**Fix:**
```bash
# Update tenant assignment
aws cognito-idp admin-update-user-attributes \
  --user-pool-id $POOL_ID \
  --username user@example.com \
  --user-attributes Name=custom:tenant_id,Value=correct-tenant
```

### Issue: Billing Not Tracking

**Check:**
1. Cost allocation tags enabled in AWS Console
2. S3 paths use tenant prefix
3. CloudWatch metrics published

**Fix:**
- Wait 24-48 hours for cost data
- Verify tags are applied: `aws resourcegroupstaggingapi get-resources`

## 📚 Quick Reference

### Onboard User
```bash
./terraform/scripts/onboard-user.sh <email> <tenant-id>
```

### Verify Tenant
```bash
./terraform/scripts/verify-tenant.sh <email>
```

### Check Billing
```bash
python terraform/scripts/get-tenant-usage.py --all-tenants --bucket <bucket> --start-date 2024-01-01 --end-date 2024-01-31
```

### Update Tenant
```bash
aws cognito-idp admin-update-user-attributes \
  --user-pool-id $POOL_ID \
  --username <email> \
  --user-attributes Name=custom:tenant_id,Value=<new-tenant>
```

## ✅ Summary

**The Complete Flow:**
1. **Terraform** creates tenant infrastructure
2. **Onboarding script** assigns user to tenant
3. **Cognito** authenticates user, issues token with tenant
4. **MCP server** extracts tenant from token
5. **S3 operations** use tenant-specific paths
6. **CloudWatch** tracks usage per tenant
7. **Billing script** calculates costs per tenant

**Key Point:** The tenant ID flows from Cognito → JWT token → Server → S3 paths → Billing. Everything is connected!
