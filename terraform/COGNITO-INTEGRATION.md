# Cognito Authentication with Tenant Billing

## Overview

This guide explains how Cognito authentication works with tenant billing and usage tracking.

**Supported Authentication Methods:**
- ✅ **Email/Password** (Username/Password) - Works out of the box
- ✅ **Google/Gmail Sign-In** (OAuth) - Requires Google OAuth setup
- ✅ **Cognito Hosted UI** - Web-based interface for both methods

All methods work identically for tenant assignment and billing!

## How It Works

### Authentication Flow

1. **User authenticates** via Cognito (email/password or Google)
2. **Cognito returns JWT token** with user attributes
3. **Application extracts tenant ID** from token attributes
4. **Requests use tenant ID** for S3 paths and billing tags
5. **Billing tracks usage** per tenant automatically

### Tenant Mapping Options

#### Option 1: User Attribute (Recommended)

Set `custom:tenant_id` attribute on Cognito user:

```python
# When creating/updating user
cognito.admin_update_user_attributes(
    UserPoolId=user_pool_id,
    Username=username,
    UserAttributes=[
        {'Name': 'custom:tenant_id', 'Value': 'acme-corp'}
    ]
)
```

#### Option 2: Email Domain Mapping

Map email domain to tenant:

```python
def get_tenant_from_email(email):
    domain = email.split('@')[1]
    # acme.com -> acme-corp
    # widget.com -> widget-inc
    domain_map = {
        'acme.com': 'acme-corp',
        'widget.com': 'widget-inc'
    }
    return domain_map.get(domain, 'default-tenant')
```

#### Option 3: Cognito Groups

Use Cognito groups for tenant assignment:

```python
# User belongs to group "tenant-acme-corp"
# Extract from token: token['cognito:groups'][0].replace('tenant-', '')
```

## Integration with MCP Server

### Extract Tenant from Cognito Token

```python
import jwt
from functools import wraps
from fastapi import Request, HTTPException

def get_tenant_from_token(request: Request) -> str:
    """Extract tenant ID from Cognito JWT token."""
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing authorization")
    
    token = auth_header.replace("Bearer ", "")
    
    # Decode token (without verification for now, verify in production!)
    decoded = jwt.decode(token, options={"verify_signature": False})
    
    # Option 1: From custom attribute
    tenant_id = decoded.get("custom:tenant_id")
    
    # Option 2: From email domain
    if not tenant_id:
        email = decoded.get("email")
        tenant_id = get_tenant_from_email(email)
    
    # Option 3: From groups
    if not tenant_id:
        groups = decoded.get("cognito:groups", [])
        if groups:
            tenant_id = groups[0].replace("tenant-", "")
    
    if not tenant_id:
        raise HTTPException(status_code=403, detail="No tenant assigned")
    
    return tenant_id

# Use in MCP server functions
@server.tool(title="Analyze trajectory")
def analyze_trajectory(
    robolog_path: str,
    request: Request,  # Add request parameter
    start_seconds: float | None = None,
    end_seconds: float | None = None
) -> list[dict[str, Any]]:
    # Get tenant from token
    tenant_id = get_tenant_from_token(request)
    
    # Use tenant-specific S3 path
    s3_path = f"s3://{os.getenv('BAGEL_S3_BUCKET')}/tenants/{tenant_id}/robologs/{robolog_path}"
    
    # Track usage
    track_tenant_usage(tenant_id, "analyze_trajectory", robolog_path)
    
    # Rest of function...
```

### Track Usage Per Tenant

```python
import boto3
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def track_tenant_usage(tenant_id: str, operation: str, resource: str):
    """Track API usage for billing."""
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
                'Unit': 'Count',
                'Timestamp': datetime.utcnow()
            }
        ]
    )
```

## Google/Gmail Sign-In Setup

### Step 1: Create Google OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create OAuth 2.0 Client ID
3. Add authorized redirect URI:
   ```
   https://your-cognito-domain.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
   ```
4. Copy Client ID and Client Secret

### Step 2: Configure in Terraform

```hcl
# terraform.tfvars
cognito_enabled = true
cognito_domain = "bagel-auth"  # Must be unique globally
google_client_id = "your-google-client-id"
google_client_secret = "your-google-client-secret"
```

### Step 3: Map Google Users to Tenants

When a user signs in with Google, you need to map them to a tenant:

**Option A: Pre-register users**
```python
# Admin script to pre-register Google users
import boto3

cognito = boto3.client('cognito-idp')

# Create user and assign tenant
cognito.admin_create_user(
    UserPoolId=user_pool_id,
    Username=google_email,
    UserAttributes=[
        {'Name': 'email', 'Value': google_email},
        {'Name': 'custom:tenant_id', 'Value': 'acme-corp'}
    ],
    MessageAction='SUPPRESS'  # Don't send welcome email
)
```

**Option B: Auto-assign based on email domain**
```python
# In Cognito pre-signup/pre-authentication Lambda trigger
def lambda_handler(event, context):
    email = event['request']['userAttributes']['email']
    domain = email.split('@')[1]
    
    # Map domain to tenant
    tenant_map = {
        'acme.com': 'acme-corp',
        'widget.com': 'widget-inc'
    }
    
    tenant_id = tenant_map.get(domain, 'default-tenant')
    
    # Set tenant attribute
    event['response']['userAttributes'] = {
        'custom:tenant_id': tenant_id
    }
    
    return event
```

## Billing Still Works!

### How Billing Tracks Cognito Users

1. **User authenticates** → Gets JWT token with `custom:tenant_id`
2. **MCP server extracts tenant** → From token attribute
3. **S3 operations use tenant path** → `s3://bucket/tenants/{tenant_id}/...`
4. **CloudWatch metrics tagged** → With tenant ID
5. **Billing script calculates costs** → Per tenant prefix

### Example Flow

```
1. User (john@acme.com) signs in with Google
   ↓
2. Cognito returns token with custom:tenant_id = "acme-corp"
   ↓
3. User calls analyze_trajectory()
   ↓
4. Server extracts tenant_id = "acme-corp" from token
   ↓
5. S3 path: s3://bucket/tenants/acme-corp/robologs/...
   ↓
6. CloudWatch metric: Tenant=acme-corp, Operation=analyze_trajectory
   ↓
7. Billing script calculates: acme-corp used 45.23 GB, $3.59
```

## Configuration

### Enable Cognito

```hcl
# terraform.tfvars
cognito_enabled = true
cognito_domain = "bagel-auth-495599747090"  # Must be globally unique
google_client_id = "your-client-id.apps.googleusercontent.com"
google_client_secret = "your-client-secret"
```

### Update ECS Environment

The ECS task definition automatically gets Cognito info:

```hcl
environment = [
  {
    name  = "COGNITO_USER_POOL_ID"
    value = aws_cognito_user_pool.bagel[0].id
  },
  {
    name  = "COGNITO_CLIENT_ID"
    value = aws_cognito_user_pool.bagel[0].user_pool_client[0].id
  }
]
```

## Security Considerations

### 1. Verify JWT Tokens

```python
import jwt
from jose import jwk, jwt as jose_jwt
from jose.utils import base64url_decode

def verify_cognito_token(token: str, user_pool_id: str, region: str):
    """Verify Cognito JWT token."""
    # Get public keys from Cognito
    keys_url = f'https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json'
    keys = requests.get(keys_url).json()['keys']
    
    # Verify token
    header = jwt.get_unverified_header(token)
    key = [k for k in keys if k['kid'] == header['kid']][0]
    
    # Decode and verify
    return jose_jwt.decode(
        token,
        key,
        algorithms=['RS256'],
        audience=client_id,
        issuer=f'https://cognito-idp.{region}.amazonaws.com/{user_pool_id}'
    )
```

### 2. Tenant Isolation

- Users can only access their tenant's S3 prefix
- IAM policies enforce boundaries
- CloudWatch metrics track per tenant

### 3. Rate Limiting

```python
from functools import lru_cache
from datetime import datetime, timedelta

@lru_cache(maxsize=1000)
def check_rate_limit(tenant_id: str, operation: str):
    """Check if tenant has exceeded rate limit."""
    # Query CloudWatch for recent API calls
    # Return True if under limit, False if exceeded
    pass
```

## Testing

### Test with Google Sign-In

1. Deploy with Cognito enabled
2. Go to Cognito Hosted UI: `https://bagel-auth.auth.us-east-1.amazoncognito.com`
3. Sign in with Google
4. Get JWT token
5. Call MCP server with token in Authorization header
6. Verify tenant is extracted correctly
7. Check S3 path uses tenant prefix
8. Verify CloudWatch metrics show tenant

## Troubleshooting

### "No tenant assigned" error

- Check user has `custom:tenant_id` attribute
- Verify token includes tenant attribute
- Check email domain mapping if using that method

### Billing not tracking

- Verify S3 paths use tenant prefix
- Check CloudWatch metrics are published
- Ensure tenant ID matches exactly (case-sensitive)

### Google sign-in not working

- Verify redirect URI matches exactly
- Check client ID/secret are correct
- Ensure Cognito domain is configured

## Summary

✅ **Yes, billing works with Cognito + Gmail!**

- Cognito users are mapped to tenants
- Tenant ID extracted from JWT token
- S3 operations use tenant-specific paths
- CloudWatch metrics track per tenant
- Billing script calculates costs per tenant

The authentication method (Cognito, Google, etc.) doesn't affect billing - as long as you extract the tenant ID from the authenticated user, everything works the same!
