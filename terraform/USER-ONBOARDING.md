# User Onboarding and Tenant Assignment Guide

## Complete System Flow

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    User Onboarding Flow                      │
└─────────────────────────────────────────────────────────────┘

1. Admin Creates Tenant
   ├── Add tenant to terraform.tfvars: tenant_names = ["acme-corp"]
   ├── Run: terraform apply
   └── Creates: S3 prefixes, IAM policies, billing tags

2. Admin Onboards User
   ├── Option A: Via Cognito Console (manual)
   ├── Option B: Via Script (automated)
   └── Assigns: custom:tenant_id attribute

3. User Authenticates
   ├── Signs in via Cognito (email/password or Google)
   └── Receives: JWT token with custom:tenant_id

4. User Makes Request
   ├── Sends: JWT token in Authorization header
   ├── Server: Extracts tenant_id from token
   ├── S3 Path: s3://bucket/tenants/{tenant_id}/...
   └── Billing: Tracks usage per tenant

5. Monthly Billing
   ├── Run: get-tenant-usage.py script
   ├── Output: CSV with costs per tenant
   └── Invoice: Each tenant based on usage
```

## Step-by-Step: Onboarding a New User

### Step 1: Create/Verify Tenant Exists

First, ensure the tenant is configured in Terraform:

```bash
# Check current tenants
grep tenant_names terraform/terraform.tfvars

# If tenant doesn't exist, add it:
# tenant_names = ["acme-corp", "widget-inc", "new-tenant"]
# Then run: terraform apply
```

### Step 2: Onboard User (Choose Method)

#### Method A: Cognito Console UI (Recommended for Manual Management)

**Best for**: Day-to-day user management, visual interface

1. Go to **AWS Console → Cognito → User Pools → bagel-prod-users**
2. Click **"Users"** tab
3. Click **"Create user"** button
4. Enter:
   - **Username**: `john@acme.com`
   - **Email**: `john@acme.com`
   - **Temporary password**: (auto-generated or set)
   - **Mark email as verified**: ✅
5. Click **"Create user"**
6. **Click on the user** you just created
7. Go to **"Attributes"** tab
8. Click **"Edit"** button
9. Click **"Add custom attribute"**
10. Enter:
    - **Name**: `custom:tenant_id`
    - **Value**: `acme-corp`
11. Click **"Save changes"**

✅ **Done!** User can now sign in and will have tenant access.

**See `COGNITO-UI-GUIDE.md` for detailed UI instructions.**

1. Go to **AWS Console → Cognito → User Pools → bagel-prod-users**
2. Click **Create user**
3. Enter:
   - **Username**: `john@acme.com`
   - **Email**: `john@acme.com`
   - **Temporary password**: (auto-generated or set)
4. Click **Create user**
5. Go to user details → **Attributes** tab
6. Click **Edit**
7. Add custom attribute:
   - **Name**: `custom:tenant_id`
   - **Value**: `acme-corp`
8. Click **Save changes**

#### Method B: AWS CLI (Scripted)

```bash
# Set variables
USER_POOL_ID=$(terraform output -raw cognito_user_pool_id)
EMAIL="john@acme.com"
TENANT_ID="acme-corp"
TEMP_PASSWORD="TempPass123!"

# Create user
aws cognito-idp admin-create-user \
  --user-pool-id $USER_POOL_ID \
  --username $EMAIL \
  --user-attributes Name=email,Value=$EMAIL Name=custom:tenant_id,Value=$TENANT_ID \
  --temporary-password $TEMP_PASSWORD \
  --message-action SUPPRESS

# User will need to change password on first login
```

#### Method C: Automated Script (Recommended)

Use the provided onboarding script (see below).

### Step 3: User First Login

1. User goes to Cognito Hosted UI or your app
2. Signs in with email/password (or Google)
3. Changes temporary password (if applicable)
4. Receives JWT token with `custom:tenant_id` attribute

### Step 4: Verify Tenant Assignment

```bash
# Test that user can access their tenant's data
# The MCP server should extract tenant from token automatically
```

## Automated Onboarding Script

### Complete Onboarding Script

```bash
#!/bin/bash
# terraform/scripts/onboard-user.sh

set -e

USER_POOL_ID="${COGNITO_USER_POOL_ID:-}"
EMAIL="${1:-}"
TENANT_ID="${2:-}"
GOOGLE_EMAIL="${3:-}"  # Optional: if using Google sign-in

if [ -z "$USER_POOL_ID" ] || [ -z "$EMAIL" ] || [ -z "$TENANT_ID" ]; then
    echo "Usage: onboard-user.sh <email> <tenant-id> [google-email]"
    echo "Example: onboard-user.sh john@acme.com acme-corp john@gmail.com"
    exit 1
fi

echo "🔐 Onboarding user: $EMAIL to tenant: $TENANT_ID"

# Generate temporary password
TEMP_PASSWORD=$(openssl rand -base64 12)

# Create user
echo "📝 Creating Cognito user..."
aws cognito-idp admin-create-user \
  --user-pool-id $USER_POOL_ID \
  --username $EMAIL \
  --user-attributes \
    Name=email,Value=$EMAIL \
    Name=email_verified,Value=true \
    Name=custom:tenant_id,Value=$TENANT_ID \
  --temporary-password $TEMP_PASSWORD \
  --message-action SUPPRESS

echo "✅ User created"

# If Google email provided, link accounts
if [ -n "$GOOGLE_EMAIL" ]; then
    echo "🔗 Linking Google account: $GOOGLE_EMAIL"
    # Note: This requires additional setup for Google provider
    echo "⚠️  Google account linking requires manual configuration"
fi

# Send welcome email (optional)
echo "📧 Sending welcome email..."
# You can use SES or another service here

echo ""
echo "✅ User onboarded successfully!"
echo "📋 Details:"
echo "   Email: $EMAIL"
echo "   Tenant: $TENANT_ID"
echo "   Temporary Password: $TEMP_PASSWORD"
echo "   ⚠️  User must change password on first login"
```

### Bulk Onboarding Script

```bash
#!/bin/bash
# terraform/scripts/onboard-users-bulk.sh

# CSV format: email,tenant_id,google_email (optional)
# users.csv:
# john@acme.com,acme-corp,john@gmail.com
# jane@widget.com,widget-inc,jane@gmail.com

USER_POOL_ID="${COGNITO_USER_POOL_ID:-}"
CSV_FILE="${1:-users.csv}"

if [ -z "$USER_POOL_ID" ]; then
    echo "Error: COGNITO_USER_POOL_ID not set"
    exit 1
fi

while IFS=',' read -r email tenant_id google_email; do
    echo "Onboarding: $email → $tenant_id"
    ./onboard-user.sh "$email" "$tenant_id" "$google_email"
    sleep 1  # Rate limiting
done < "$CSV_FILE"
```

## Tenant Assignment Strategies

### Strategy 1: Manual Assignment (Recommended for Small Scale)

**Best for**: < 50 users, clear tenant boundaries

**Process**:
1. Admin determines tenant based on company/org
2. Manually assigns `custom:tenant_id` attribute
3. User gets access to that tenant's data

**Example**:
```bash
# John works for Acme Corp
onboard-user.sh john@acme.com acme-corp

# Jane works for Widget Inc
onboard-user.sh jane@widget.com widget-inc
```

### Strategy 2: Email Domain Mapping (Automated)

**Best for**: Company email domains, automated onboarding

**Process**:
1. Map email domains to tenants
2. Auto-assign tenant on user creation
3. Use Lambda trigger or script

**Implementation**:

```python
# Lambda function for Cognito pre-signup trigger
import json

TENANT_MAP = {
    'acme.com': 'acme-corp',
    'widget.com': 'widget-inc',
    'techstartup.io': 'tech-startup'
}

def lambda_handler(event, context):
    email = event['request']['userAttributes'].get('email', '')
    domain = email.split('@')[1] if '@' in email else ''
    
    tenant_id = TENANT_MAP.get(domain, 'default-tenant')
    
    event['response']['userAttributes'] = {
        'custom:tenant_id': tenant_id
    }
    
    return event
```

### Strategy 3: Invitation-Based (Self-Service)

**Best for**: Multi-tenant SaaS, users invite themselves

**Process**:
1. Admin creates tenant invitation link
2. User clicks link, signs up
3. Tenant assigned from invitation token

**Implementation**:

```python
# Generate invitation
import secrets
import boto3

dynamodb = boto3.client('dynamodb')

def create_tenant_invitation(tenant_id, expires_days=7):
    invitation_token = secrets.token_urlsafe(32)
    
    dynamodb.put_item(
        TableName='tenant-invitations',
        Item={
            'token': {'S': invitation_token},
            'tenant_id': {'S': tenant_id},
            'expires_at': {'N': str(int(time.time()) + expires_days * 86400)},
            'used': {'BOOL': False}
        }
    )
    
    return f"https://your-app.com/signup?invite={invitation_token}"

# On signup, check invitation
def assign_tenant_from_invitation(invitation_token):
    response = dynamodb.get_item(
        TableName='tenant-invitations',
        Key={'token': {'S': invitation_token}}
    )
    
    if response.get('Item') and not response['Item']['used']['BOOL']:
        tenant_id = response['Item']['tenant_id']['S']
        # Mark as used
        dynamodb.update_item(...)
        return tenant_id
    
    return None
```

## Verification and Testing

### Verify User Has Correct Tenant

```bash
# Get user attributes
aws cognito-idp admin-get-user \
  --user-pool-id $USER_POOL_ID \
  --username john@acme.com

# Should show: custom:tenant_id = acme-corp
```

### Test Tenant Isolation

```python
# Test script
import requests
import jwt

# User 1 (acme-corp)
token1 = get_cognito_token("john@acme.com", "password")
response1 = requests.get(
    "https://your-mcp-server/analyze_trajectory",
    headers={"Authorization": f"Bearer {token1}"},
    params={"robolog_path": "test.ulg"}
)
# Should access: s3://bucket/tenants/acme-corp/...

# User 2 (widget-inc) - should NOT access acme-corp data
token2 = get_cognito_token("jane@widget.com", "password")
response2 = requests.get(
    "https://your-mcp-server/analyze_trajectory",
    headers={"Authorization": f"Bearer {token2}"},
    params={"robolog_path": "test.ulg"}
)
# Should access: s3://bucket/tenants/widget-inc/...
# Should NOT be able to access acme-corp data
```

## Common Onboarding Scenarios

### Scenario 1: New Company Signs Up

```bash
# 1. Add tenant to Terraform
# terraform.tfvars: tenant_names = [..., "new-company"]

# 2. Apply Terraform
terraform apply

# 3. Onboard first user (admin)
./onboard-user.sh admin@new-company.com new-company

# 4. Admin can now onboard other users via your app
```

### Scenario 2: User Switches Companies

```bash
# Update tenant assignment
aws cognito-idp admin-update-user-attributes \
  --user-pool-id $USER_POOL_ID \
  --username john@example.com \
  --user-attributes Name=custom:tenant_id,Value=new-company

# User's data access changes immediately
# Old data remains in old tenant's S3 prefix
# New data goes to new tenant's prefix
```

### Scenario 3: Bulk Import from CSV

```bash
# users.csv:
# email,tenant_id
# user1@acme.com,acme-corp
# user2@acme.com,acme-corp
# user3@widget.com,widget-inc

./onboard-users-bulk.sh users.csv
```

## Troubleshooting

### User Can't Access Data

1. **Check tenant assignment**:
   ```bash
   aws cognito-idp admin-get-user \
     --user-pool-id $USER_POOL_ID \
     --username user@example.com \
     --query 'UserAttributes[?Name==`custom:tenant_id`].Value'
   ```

2. **Verify tenant exists in Terraform**:
   ```bash
   grep -A 1 tenant_names terraform/terraform.tfvars
   ```

3. **Check S3 prefix exists**:
   ```bash
   aws s3 ls s3://bucket/tenants/tenant-id/
   ```

### Wrong Tenant Assigned

```bash
# Update tenant
aws cognito-idp admin-update-user-attributes \
  --user-pool-id $USER_POOL_ID \
  --username user@example.com \
  --user-attributes Name=custom:tenant_id,Value=correct-tenant
```

### User Can See Other Tenant's Data

1. **Check IAM policies** - Verify tenant isolation
2. **Check application logic** - Ensure tenant extraction works
3. **Review S3 paths** - Verify tenant prefix is used

## Best Practices

### 1. Tenant Naming Convention

Use consistent naming:
- ✅ `acme-corp`, `widget-inc`, `tech-startup`
- ❌ `Acme Corp`, `widget_inc`, `tech-startup-2024`

### 2. Validation

Always validate tenant exists before assignment:

```python
def validate_tenant(tenant_id):
    allowed_tenants = os.getenv('BAGEL_TENANT_NAMES', '').split(',')
    if tenant_id not in allowed_tenants:
        raise ValueError(f"Invalid tenant: {tenant_id}")
    return True
```

### 3. Audit Trail

Log all tenant assignments:

```python
import logging

logger.info(f"User {email} assigned to tenant {tenant_id}", extra={
    'user': email,
    'tenant': tenant_id,
    'assigned_by': admin_user,
    'timestamp': datetime.utcnow()
})
```

### 4. Onboarding Checklist

- [ ] Tenant exists in Terraform config
- [ ] Terraform applied (S3 prefixes created)
- [ ] User created in Cognito
- [ ] Tenant attribute assigned
- [ ] User can authenticate
- [ ] User can access their tenant's data
- [ ] Billing tracking enabled
- [ ] Access logs reviewed

## Summary

**Complete Flow**:
1. **Admin** adds tenant to Terraform → `terraform apply`
2. **Admin** onboards user → Assigns `custom:tenant_id`
3. **User** authenticates → Gets JWT with tenant ID
4. **System** uses tenant ID → For S3 paths and billing
5. **Billing** tracks usage → Per tenant automatically

**Key Points**:
- Tenant must exist in Terraform first
- User must have `custom:tenant_id` attribute
- Tenant ID is extracted from JWT token
- S3 paths automatically use tenant prefix
- Billing tracks per tenant automatically
