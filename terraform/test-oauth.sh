#!/bin/bash
# Test OAuth configuration for Cognito

set -e

echo "🔍 Testing OAuth Configuration"
echo "=============================="
echo ""

# Get Cognito details from Terraform
if command -v terraform &> /dev/null; then
    cd "$(dirname "$0")/.."
    
    USER_POOL_ID=$(terraform output -raw cognito_user_pool_id 2>/dev/null || echo "")
    CLIENT_ID=$(terraform output -raw cognito_user_pool_client_id 2>/dev/null || echo "")
    DOMAIN=$(terraform output -raw cognito_domain 2>/dev/null || echo "")
    REGION=$(grep '^aws_region' terraform.tfvars | head -1 | sed 's/.*= *"\(.*\)".*/\1/' | sed 's/.*= *\(.*\)/\1/' || echo "us-east-1")
else
    echo "⚠️  Terraform not found, using environment variables"
    USER_POOL_ID="${COGNITO_USER_POOL_ID:-}"
    CLIENT_ID="${COGNITO_CLIENT_ID:-}"
    DOMAIN="${COGNITO_DOMAIN:-}"
    REGION="${AWS_REGION:-us-east-1}"
fi

if [ -z "$USER_POOL_ID" ] || [ -z "$CLIENT_ID" ]; then
    echo "❌ Cognito not configured or not deployed"
    echo "   Set COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID"
    echo "   Or run: terraform apply (with cognito_enabled = true)"
    exit 1
fi

echo "✅ Cognito Configuration:"
echo "   User Pool ID: $USER_POOL_ID"
echo "   Client ID: $CLIENT_ID"
[ -n "$DOMAIN" ] && echo "   Domain: $DOMAIN"
echo ""

# Check OAuth configuration
echo "🔍 Checking OAuth Configuration..."
CLIENT_INFO=$(aws cognito-idp describe-user-pool-client \
    --user-pool-id "$USER_POOL_ID" \
    --client-id "$CLIENT_ID" \
    --region "$REGION" 2>/dev/null || echo "")

if [ -z "$CLIENT_INFO" ]; then
    echo "❌ Could not retrieve client information"
    exit 1
fi

# Check OAuth flows
OAUTH_FLOWS=$(echo "$CLIENT_INFO" | jq -r '.UserPoolClient.AllowedOAuthFlows[]' 2>/dev/null || echo "")
OAUTH_SCOPES=$(echo "$CLIENT_INFO" | jq -r '.UserPoolClient.AllowedOAuthScopes[]' 2>/dev/null || echo "")
OAUTH_ENABLED=$(echo "$CLIENT_INFO" | jq -r '.UserPoolClient.AllowedOAuthFlowsUserPoolClient' 2>/dev/null || echo "false")

echo "📋 OAuth Settings:"
echo "   OAuth Enabled: $OAUTH_ENABLED"
echo "   OAuth Flows:"
for flow in $OAUTH_FLOWS; do
    echo "     - $flow"
done
echo "   OAuth Scopes:"
for scope in $OAUTH_SCOPES; do
    echo "     - $scope"
done
echo ""

# Check callback URLs
CALLBACK_URLS=$(echo "$CLIENT_INFO" | jq -r '.UserPoolClient.CallbackURLs[]' 2>/dev/null || echo "")
echo "📋 Callback URLs:"
for url in $CALLBACK_URLS; do
    echo "     - $url"
done
echo ""

# Check domain
if [ -n "$DOMAIN" ]; then
    echo "🌐 Hosted UI Domain:"
    echo "   https://$DOMAIN"
    echo ""
    echo "🔗 OAuth URLs:"
    echo "   Authorization: https://$DOMAIN/oauth2/authorize"
    echo "   Token: https://$DOMAIN/oauth2/token"
    echo ""
    
    # Test domain accessibility
    echo "🔍 Testing domain..."
    if curl -s -o /dev/null -w "%{http_code}" "https://$DOMAIN" | grep -q "200\|302"; then
        echo "   ✅ Domain is accessible"
    else
        echo "   ⚠️  Domain may not be fully configured yet"
    fi
else
    echo "⚠️  No domain configured"
    echo "   Set cognito_domain in terraform.tfvars to enable Hosted UI"
fi

echo ""
echo "📝 OAuth Test URLs:"
echo ""

# Generate test authorization URL
if [ -n "$DOMAIN" ] && [ -n "$CLIENT_ID" ]; then
    REDIRECT_URI="http://localhost:8000/callback"
    SCOPE="email+openid+profile"
    RESPONSE_TYPE="code"
    
    AUTH_URL="https://$DOMAIN/oauth2/authorize?client_id=$CLIENT_ID&response_type=$RESPONSE_TYPE&scope=$SCOPE&redirect_uri=$REDIRECT_URI"
    
    echo "🔗 Authorization URL (for testing):"
    echo "   $AUTH_URL"
    echo ""
    echo "💡 To test:"
    echo "   1. Open the URL above in a browser"
    echo "   2. Sign in (email/password or Google)"
    echo "   3. You'll be redirected with an authorization code"
    echo "   4. Exchange code for tokens"
fi

echo ""
echo "✅ OAuth Configuration Check Complete"
