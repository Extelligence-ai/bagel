#!/bin/bash
# User onboarding script for Bagel MCP Server
# Usage: ./onboard-user.sh <email> <tenant-id> [google-email]

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Get parameters
EMAIL="${1:-}"
TENANT_ID="${2:-}"
GOOGLE_EMAIL="${3:-}"

# Get Cognito User Pool ID from Terraform
if [ -z "$COGNITO_USER_POOL_ID" ]; then
    if command -v terraform &> /dev/null; then
        COGNITO_USER_POOL_ID=$(cd "$(dirname "$0")/.." && terraform output -raw cognito_user_pool_id 2>/dev/null || echo "")
    fi
fi

# Validation
if [ -z "$EMAIL" ] || [ -z "$TENANT_ID" ]; then
    echo -e "${RED}Error: Missing required parameters${NC}"
    echo ""
    echo "Usage: $0 <email> <tenant-id> [google-email]"
    echo ""
    echo "Examples:"
    echo "  $0 john@acme.com acme-corp"
    echo "  $0 jane@widget.com widget-inc jane@gmail.com"
    echo ""
    exit 1
fi

if [ -z "$COGNITO_USER_POOL_ID" ]; then
    echo -e "${RED}Error: COGNITO_USER_POOL_ID not set${NC}"
    echo "Set it manually or run from terraform directory"
    exit 1
fi

echo -e "${GREEN}🔐 Onboarding User${NC}"
echo "=================="
echo "Email: $EMAIL"
echo "Tenant: $TENANT_ID"
[ -n "$GOOGLE_EMAIL" ] && echo "Google Email: $GOOGLE_EMAIL"
echo ""

# Check if tenant exists in Terraform config
if command -v terraform &> /dev/null; then
    cd "$(dirname "$0")/.."
    TENANT_LIST=$(grep -A 10 "tenant_names" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | tr '\n' ' ')
    if [[ ! "$TENANT_LIST" =~ $TENANT_ID ]]; then
        echo -e "${YELLOW}⚠️  Warning: Tenant '$TENANT_ID' not found in terraform.tfvars${NC}"
        echo "   Found tenants: $TENANT_LIST"
        echo "   Continuing anyway, but tenant may not be configured..."
        echo ""
    fi
fi

# Generate temporary password
TEMP_PASSWORD=$(openssl rand -base64 12 | tr -d "=+/" | cut -c1-12)

echo -e "${YELLOW}📝 Creating Cognito user...${NC}"

# Check if user already exists
if aws cognito-idp admin-get-user \
    --user-pool-id "$COGNITO_USER_POOL_ID" \
    --username "$EMAIL" &>/dev/null; then
    echo -e "${YELLOW}⚠️  User already exists, updating tenant assignment...${NC}"
    
    # Update tenant attribute
    aws cognito-idp admin-update-user-attributes \
        --user-pool-id "$COGNITO_USER_POOL_ID" \
        --username "$EMAIL" \
        --user-attributes Name=custom:tenant_id,Value="$TENANT_ID"
    
    echo -e "${GREEN}✅ Tenant assignment updated${NC}"
else
    # Create new user
    aws cognito-idp admin-create-user \
        --user-pool-id "$COGNITO_USER_POOL_ID" \
        --username "$EMAIL" \
        --user-attributes \
            Name=email,Value="$EMAIL" \
            Name=email_verified,Value=true \
            Name=custom:tenant_id,Value="$TENANT_ID" \
        --temporary-password "$TEMP_PASSWORD" \
        --message-action SUPPRESS
    
    echo -e "${GREEN}✅ User created${NC}"
    echo ""
    echo -e "${YELLOW}📧 Temporary Password: ${TEMP_PASSWORD}${NC}"
    echo -e "${YELLOW}⚠️  User must change password on first login${NC}"
fi

# Verify tenant assignment
echo ""
echo -e "${YELLOW}🔍 Verifying tenant assignment...${NC}"
TENANT_ATTR=$(aws cognito-idp admin-get-user \
    --user-pool-id "$COGNITO_USER_POOL_ID" \
    --username "$EMAIL" \
    --query 'UserAttributes[?Name==`custom:tenant_id`].Value' \
    --output text)

if [ "$TENANT_ATTR" = "$TENANT_ID" ]; then
    echo -e "${GREEN}✅ Tenant assignment verified: $TENANT_ATTR${NC}"
else
    echo -e "${RED}❌ Tenant assignment mismatch!${NC}"
    echo "   Expected: $TENANT_ID"
    echo "   Found: $TENANT_ATTR"
    exit 1
fi

# Summary
echo ""
echo -e "${GREEN}✅ User onboarding complete!${NC}"
echo ""
echo "📋 Summary:"
echo "   Email: $EMAIL"
echo "   Tenant: $TENANT_ID"
[ -n "$GOOGLE_EMAIL" ] && echo "   Google Email: $GOOGLE_EMAIL"
echo "   User Pool ID: $COGNITO_USER_POOL_ID"
echo ""
echo "🔗 Next Steps:"
echo "   1. User should sign in and change password"
echo "   2. User will receive JWT token with tenant_id"
echo "   3. All requests will use tenant-specific S3 paths"
echo "   4. Usage will be tracked for billing"
