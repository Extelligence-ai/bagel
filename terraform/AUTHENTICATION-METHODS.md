# Authentication Methods Guide

## Supported Authentication Methods

Bagel MCP Server supports **multiple authentication methods**:

1. ✅ **Email/Password** (Username/Password)
2. ✅ **Google/Gmail Sign-In** (OAuth)
3. ✅ **Cognito Hosted UI** (Web-based)

All methods work the same way for tenant assignment and billing!

## How Each Method Works

### Method 1: Email/Password (Username/Password)

**Configuration:**
- ✅ Enabled by default in Cognito
- ✅ Users sign in with their email address as username
- ✅ Password requirements: 8+ chars, uppercase, lowercase, number, symbol

**User Flow:**
```
1. User enters: email + password
2. Cognito authenticates
3. Returns JWT token with custom:tenant_id
4. User makes API calls with token
```

**Code Example:**
```python
import boto3

cognito = boto3.client('cognito-idp')

# Sign in with email/password
response = cognito.initiate_auth(
    ClientId='your-client-id',
    AuthFlow='USER_PASSWORD_AUTH',  # or 'USER_SRP_AUTH'
    AuthParameters={
        'USERNAME': 'john@acme.com',
        'PASSWORD': 'SecurePass123!'
    }
)

token = response['AuthenticationResult']['IdToken']
# Token contains: {"email": "john@acme.com", "custom:tenant_id": "acme-corp"}
```

**Via Cognito Hosted UI:**
```
1. User goes to: https://bagel-auth.auth.us-east-1.amazoncognito.com
2. Enters email and password
3. Signs in
4. Redirected with JWT token
```

### Method 2: Google/Gmail Sign-In

**Configuration:**
- ⚙️ Requires Google OAuth setup
- ✅ Configured in `terraform.tfvars`:
  ```hcl
  cognito_enabled = true
  google_client_id = "your-client-id.apps.googleusercontent.com"
  google_client_secret = "your-client-secret"
  ```

**User Flow:**
```
1. User clicks "Sign in with Google"
2. Redirected to Google OAuth
3. User authorizes
4. Cognito receives Google token
5. Returns JWT token with custom:tenant_id
6. User makes API calls with token
```

**Code Example:**
```python
# Via Cognito Hosted UI - user clicks "Sign in with Google"
# Or programmatically:

# 1. Get authorization URL
auth_url = f"https://{cognito_domain}/oauth2/authorize?" \
           f"client_id={client_id}&" \
           f"response_type=code&" \
           f"scope=email+openid+profile&" \
           f"redirect_uri={redirect_uri}"

# 2. User authorizes, gets code
# 3. Exchange code for tokens
response = cognito.initiate_auth(
    ClientId=client_id,
    AuthFlow='authorization_code',
    AuthParameters={
        'CODE': authorization_code,
        'REDIRECT_URI': redirect_uri
    }
)

token = response['AuthenticationResult']['IdToken']
# Token contains: {"email": "john@gmail.com", "custom:tenant_id": "acme-corp"}
```

### Method 3: Cognito Hosted UI (Web-Based)

**Configuration:**
- ✅ Automatically available when Cognito is enabled
- ✅ Supports both email/password and Google sign-in
- ✅ Customizable UI

**User Flow:**
```
1. User visits: https://bagel-auth.auth.us-east-1.amazoncognito.com
2. Sees sign-in form with options:
   - Email/Password form
   - "Sign in with Google" button
3. User chooses method and signs in
4. Redirected with JWT token
```

## Tenant Assignment Works the Same

**Regardless of authentication method**, the tenant assignment works identically:

1. **User is onboarded** → `custom:tenant_id` attribute set
2. **User authenticates** → Via any method (email/password, Google, etc.)
3. **Cognito issues JWT** → Token includes `custom:tenant_id`
4. **Server extracts tenant** → From token attribute
5. **Operations use tenant** → S3 paths, billing, etc.

## Configuration Details

### Email/Password Settings

In `cognito.tf`, email/password is configured as:

```hcl
# Username is email
username_attributes = ["email"]

# Password policy
password_policy {
  minimum_length    = 8
  require_lowercase = true
  require_numbers   = true
  require_symbols   = true
  require_uppercase = true
}

# Auth flows enabled
explicit_auth_flows = [
  "ALLOW_USER_PASSWORD_AUTH",  # ✅ Email/password
  "ALLOW_USER_SRP_AUTH",       # ✅ Secure Remote Password
  "ALLOW_REFRESH_TOKEN_AUTH"   # ✅ Token refresh
]
```

### Google Sign-In Settings

```hcl
# Google identity provider (optional)
identity_provider {
  provider_name = "Google"
  provider_type = "Google"
  
  provider_details = {
    client_id     = var.google_client_id
    client_secret = var.google_client_secret
  }
}

# App client supports both
supported_identity_providers = ["COGNITO", "Google"]
```

## Examples

### Example 1: Sign In with Email/Password

```python
import boto3
import requests

# Initialize Cognito client
cognito = boto3.client('cognito-idp', region_name='us-east-1')

# Sign in
response = cognito.initiate_auth(
    ClientId='your-client-id',
    AuthFlow='USER_PASSWORD_AUTH',
    AuthParameters={
        'USERNAME': 'john@acme.com',
        'PASSWORD': 'MySecurePass123!'
    }
)

token = response['AuthenticationResult']['IdToken']

# Use token to call MCP server
response = requests.post(
    'https://your-mcp-server/tools/analyze_trajectory',
    headers={'Authorization': f'Bearer {token}'},
    json={'robolog_path': 'race_1.bag'}
)
```

### Example 2: Sign In with Google

```python
# Via Hosted UI (easiest)
# User visits: https://bagel-auth.auth.us-east-1.amazoncognito.com
# Clicks "Sign in with Google"
# Gets redirected with token

# Or programmatically (more complex, requires OAuth flow)
# See COGNITO-INTEGRATION.md for full OAuth flow
```

### Example 3: Sign In via Hosted UI

```html
<!-- In your web app -->
<a href="https://bagel-auth.auth.us-east-1.amazoncognito.com/login?
  client_id=your-client-id&
  response_type=code&
  redirect_uri=https://your-app.com/callback&
  scope=email+openid+profile">
  Sign In
</a>
```

## First-Time User Experience

### Email/Password Users

1. **Admin onboards user** → Creates account with temporary password
2. **User receives email** (if email sending configured) or gets password from admin
3. **User signs in** → Enters email and temporary password
4. **Cognito requires password change** → User sets new password
5. **User can now use system** → Gets JWT token with tenant ID

### Google Users

1. **Admin onboards user** → Creates account, sets `custom:tenant_id`
2. **User clicks "Sign in with Google"** → On hosted UI or your app
3. **Google OAuth flow** → User authorizes
4. **Cognito links accounts** → Maps Google user to Cognito user
5. **User gets JWT token** → With tenant ID included

## Password Reset

### Via Hosted UI

```
1. User clicks "Forgot password"
2. Enters email
3. Receives reset code
4. Enters new password
5. Signs in with new password
```

### Via API

```python
# Initiate password reset
cognito.forgot_password(
    ClientId=client_id,
    Username='john@acme.com'
)

# User receives code, then:
cognito.confirm_forgot_password(
    ClientId=client_id,
    Username='john@acme.com',
    ConfirmationCode='123456',
    Password='NewSecurePass123!'
)
```

## Security Features

### All Methods Include:

- ✅ **JWT tokens** with expiration
- ✅ **Refresh tokens** for long sessions
- ✅ **Token verification** (signature validation)
- ✅ **Password encryption** (Cognito handles)
- ✅ **Multi-factor authentication** (optional, can be enabled)

### Tenant Isolation:

- ✅ **Same for all methods** - tenant ID in token
- ✅ **S3 access** restricted to tenant prefix
- ✅ **IAM policies** enforce boundaries
- ✅ **Billing** tracks per tenant

## Testing Authentication

### Test Email/Password

```bash
# Create test user
./onboard-user.sh test@example.com test-tenant

# Sign in via CLI
aws cognito-idp initiate-auth \
  --client-id $CLIENT_ID \
  --auth-flow USER_PASSWORD_AUTH \
  --auth-parameters USERNAME=test@example.com,PASSWORD=TempPass123!
```

### Test Google Sign-In

1. Go to Cognito Hosted UI
2. Click "Sign in with Google"
3. Complete OAuth flow
4. Verify token contains tenant ID

## Summary

✅ **Email/Password**: Works out of the box, no additional setup  
✅ **Google/Gmail**: Works with OAuth credentials  
✅ **Hosted UI**: Works for both methods  
✅ **Tenant Assignment**: Same for all methods  
✅ **Billing**: Works the same regardless of auth method  

**Key Point**: The authentication method doesn't matter - as long as the user has `custom:tenant_id` set, everything works identically!
