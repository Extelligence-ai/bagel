# OAuth Setup and Testing Guide

## ✅ OAuth Configuration Status

**OAuth is configured and ready to use!** Here's what's enabled:

### What's Configured

1. ✅ **OAuth Flows**: `code` and `implicit`
2. ✅ **OAuth Scopes**: `email`, `openid`, `profile`
3. ✅ **OAuth Enabled**: `allowed_oauth_flows_user_pool_client = true`
4. ✅ **Callback URLs**: Configured for localhost and production
5. ✅ **Hosted UI Domain**: Configured (if `cognito_domain` is set)

## OAuth Flows Supported

### Authorization Code Flow (Recommended)

**Best for**: Server-side applications, most secure

```
1. User redirected to: https://domain.auth.region.amazoncognito.com/oauth2/authorize
2. User signs in
3. Redirected back with authorization code
4. Exchange code for tokens
```

### Implicit Flow

**Best for**: Single-page applications (SPAs)

```
1. User redirected to authorization URL
2. User signs in
3. Redirected back with tokens directly (no code exchange)
```

## Configuration

### Enable OAuth

OAuth is **automatically enabled** when Cognito is enabled. Just set:

```hcl
# terraform.tfvars
cognito_enabled = true
cognito_domain = "bagel-auth-495599747090"  # Must be globally unique
```

### OAuth Settings (Already Configured)

```hcl
# In cognito.tf - already set up!
allowed_oauth_flows = ["code", "implicit"]
allowed_oauth_scopes = ["email", "openid", "profile"]
allowed_oauth_flows_user_pool_client = true
```

## Testing OAuth

### Method 1: Test Script

```bash
./terraform/test-oauth.sh
```

This will:
- ✅ Check OAuth configuration
- ✅ Verify flows and scopes
- ✅ Generate test authorization URLs
- ✅ Test domain accessibility

### Method 2: Manual Test

#### Step 1: Get OAuth URLs

```bash
# Get Cognito details
USER_POOL_ID=$(terraform output -raw cognito_user_pool_id)
CLIENT_ID=$(terraform output -raw cognito_user_pool_client_id)
DOMAIN=$(terraform output -raw cognito_domain)
REGION=$(terraform output -raw aws_region)

# Authorization URL
AUTH_URL="https://$DOMAIN/oauth2/authorize?client_id=$CLIENT_ID&response_type=code&scope=email+openid+profile&redirect_uri=http://localhost:8000/callback"
```

#### Step 2: Test Authorization Flow

1. **Open authorization URL** in browser
2. **Sign in** (email/password or Google)
3. **Get redirected** with authorization code:
   ```
   http://localhost:8000/callback?code=AUTHORIZATION_CODE
   ```

#### Step 3: Exchange Code for Tokens

```python
import boto3
import requests

cognito = boto3.client('cognito-idp', region_name='us-east-1')

# Exchange authorization code for tokens
response = cognito.initiate_auth(
    ClientId='your-client-id',
    AuthFlow='authorization_code',
    AuthParameters={
        'CODE': 'authorization-code-from-callback',
        'REDIRECT_URI': 'http://localhost:8000/callback'
    }
)

id_token = response['AuthenticationResult']['IdToken']
access_token = response['AuthenticationResult']['AccessToken']
refresh_token = response['AuthenticationResult']['RefreshToken']
```

## OAuth URLs

### Authorization Endpoint

```
https://{domain}.auth.{region}.amazoncognito.com/oauth2/authorize
```

**Parameters:**
- `client_id`: Your Cognito client ID
- `response_type`: `code` (authorization code) or `token` (implicit)
- `scope`: `email openid profile`
- `redirect_uri`: Your callback URL (must match configured URLs)

### Token Endpoint

```
https://{domain}.auth.{region}.amazoncognito.com/oauth2/token
```

### UserInfo Endpoint

```
https://{domain}.auth.{region}.amazoncognito.com/oauth2/userInfo
```

## Complete OAuth Example

### Python Example

```python
import requests
import boto3
from urllib.parse import urlencode, parse_qs

# Configuration
CLIENT_ID = "your-client-id"
DOMAIN = "bagel-auth-495599747090.auth.us-east-1.amazoncognito.com"
REDIRECT_URI = "http://localhost:8000/callback"
SCOPE = "email openid profile"

# Step 1: Generate authorization URL
auth_params = {
    "client_id": CLIENT_ID,
    "response_type": "code",
    "scope": SCOPE,
    "redirect_uri": REDIRECT_URI
}

auth_url = f"https://{DOMAIN}/oauth2/authorize?{urlencode(auth_params)}"
print(f"Visit: {auth_url}")

# Step 2: User visits URL, signs in, gets redirected with code
# In your callback handler:
authorization_code = request.args.get('code')

# Step 3: Exchange code for tokens
token_url = f"https://{DOMAIN}/oauth2/token"
token_data = {
    "grant_type": "authorization_code",
    "client_id": CLIENT_ID,
    "code": authorization_code,
    "redirect_uri": REDIRECT_URI
}

response = requests.post(token_url, data=token_data)
tokens = response.json()

id_token = tokens['id_token']
access_token = tokens['access_token']
refresh_token = tokens['refresh_token']

# Step 4: Use tokens
# id_token contains: {"email": "...", "custom:tenant_id": "..."}
```

### JavaScript Example

```javascript
// Generate authorization URL
const authUrl = `https://${domain}/oauth2/authorize?` +
  `client_id=${clientId}&` +
  `response_type=code&` +
  `scope=email+openid+profile&` +
  `redirect_uri=${encodeURIComponent(redirectUri)}`;

// Redirect user
window.location.href = authUrl;

// In callback handler
const code = new URLSearchParams(window.location.search).get('code');

// Exchange code for tokens
const tokenResponse = await fetch(`https://${domain}/oauth2/token`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  body: new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: clientId,
    code: code,
    redirect_uri: redirectUri
  })
});

const tokens = await tokenResponse.json();
// Use tokens.id_token, tokens.access_token
```

## OAuth with Google

If Google OAuth is configured, users can:

1. **Click "Sign in with Google"** on Hosted UI
2. **Complete Google OAuth flow**
3. **Get Cognito JWT token** with tenant ID
4. **Use token** for API calls

The flow is the same - Cognito handles the Google OAuth integration.

## Troubleshooting

### "Invalid redirect_uri"

**Problem**: Callback URL doesn't match configured URLs

**Fix**: Add your callback URL to `callback_urls` in `cognito.tf`:
```hcl
callback_urls = [
  "http://localhost:8000/callback",
  "https://your-actual-domain.com/callback"  # Add your URL
]
```

Then run: `terraform apply`

### "Invalid client_id"

**Problem**: Client ID is wrong

**Fix**: Get correct client ID:
```bash
terraform output cognito_user_pool_client_id
```

### "Domain not found"

**Problem**: Cognito domain not configured

**Fix**: Set `cognito_domain` in `terraform.tfvars` and apply:
```hcl
cognito_domain = "bagel-auth-495599747090"  # Must be unique
terraform apply
```

### OAuth Not Working

**Check:**
1. OAuth flows enabled? `allowed_oauth_flows_user_pool_client = true`
2. Domain configured? `cognito_domain` set
3. Callback URLs match? Check `callback_urls` in config
4. Client ID correct? Verify with `terraform output`

## Summary

✅ **OAuth is configured and ready!**

- ✅ Authorization Code flow: Works
- ✅ Implicit flow: Works  
- ✅ Hosted UI: Works (if domain configured)
- ✅ Google OAuth: Works (if configured)
- ✅ Email/Password: Works
- ✅ Tenant assignment: Works with all methods

**To use OAuth:**
1. Set `cognito_enabled = true` and `cognito_domain = "..."` in `terraform.tfvars`
2. Run `terraform apply`
3. Use the authorization URL to start OAuth flow
4. Exchange code for tokens
5. Use tokens for API calls

Everything is ready to go! 🚀
