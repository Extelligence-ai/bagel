#!/bin/bash
# Verify tenant assignment for a user
# Usage: ./verify-tenant.sh <email>

set -e

EMAIL="${1:-}"

if [ -z "$EMAIL" ]; then
    echo "Usage: $0 <email>"
    exit 1
fi

if [ -z "$COGNITO_USER_POOL_ID" ]; then
    if command -v terraform &> /dev/null; then
        COGNITO_USER_POOL_ID=$(cd "$(dirname "$0")/.." && terraform output -raw cognito_user_pool_id 2>/dev/null || echo "")
    fi
fi

if [ -z "$COGNITO_USER_POOL_ID" ]; then
    echo "Error: COGNITO_USER_POOL_ID not set"
    exit 1
fi

echo "🔍 Verifying tenant for: $EMAIL"
echo ""

# Get user info
USER_INFO=$(aws cognito-idp admin-get-user \
    --user-pool-id "$COGNITO_USER_POOL_ID" \
    --username "$EMAIL" 2>/dev/null || echo "")

if [ -z "$USER_INFO" ]; then
    echo "❌ User not found"
    exit 1
fi

# Extract tenant ID
TENANT_ID=$(echo "$USER_INFO" | \
    jq -r '.UserAttributes[] | select(.Name=="custom:tenant_id") | .Value' 2>/dev/null || echo "")

# Extract email
USER_EMAIL=$(echo "$USER_INFO" | \
    jq -r '.UserAttributes[] | select(.Name=="email") | .Value' 2>/dev/null || echo "")

# Extract status
USER_STATUS=$(echo "$USER_INFO" | jq -r '.UserStatus' 2>/dev/null || echo "")

echo "📋 User Information:"
echo "   Email: $USER_EMAIL"
echo "   Status: $USER_STATUS"
echo "   Tenant ID: ${TENANT_ID:-❌ NOT SET}"

if [ -z "$TENANT_ID" ]; then
    echo ""
    echo "⚠️  Warning: User has no tenant assigned!"
    echo "   Run: ./onboard-user.sh $EMAIL <tenant-id>"
    exit 1
fi

# Check if tenant exists in S3
BUCKET_NAME=$(cd "$(dirname "$0")/.." && terraform output -raw s3_bucket_name 2>/dev/null || echo "")
if [ -n "$BUCKET_NAME" ]; then
    echo ""
    echo "🔍 Checking S3 tenant prefix..."
    if aws s3 ls "s3://$BUCKET_NAME/tenants/$TENANT_ID/" &>/dev/null; then
        echo "   ✅ S3 prefix exists: s3://$BUCKET_NAME/tenants/$TENANT_ID/"
    else
        echo "   ⚠️  S3 prefix does not exist (may be empty)"
    fi
fi

echo ""
echo "✅ Verification complete"
