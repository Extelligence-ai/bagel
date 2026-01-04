# Cognito User Pool for Authentication
# Supports Gmail (Google) and other identity providers

variable "cognito_enabled" {
  description = "Enable Cognito authentication"
  type        = bool
  default     = false
}

variable "cognito_domain" {
  description = "Cognito domain prefix (must be unique globally)"
  type        = string
  default     = ""
}

variable "google_client_id" {
  description = "Google OAuth client ID (if using Gmail/Google sign-in)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "google_client_secret" {
  description = "Google OAuth client secret"
  type        = string
  default     = ""
  sensitive   = true
}

# Cognito User Pool
resource "aws_cognito_user_pool" "bagel" {
  count = var.cognito_enabled ? 1 : 0
  name  = "${var.project_name}-${var.environment}-users"

  # Username configuration
  username_attributes      = ["email"]
  auto_verified_attributes = ["email"]

  # Password policy
  password_policy {
    minimum_length    = 8
    require_lowercase = true
    require_numbers   = true
    require_symbols   = true
    require_uppercase = true
  }

  # User attributes for tenant mapping
  schema {
    name                = "tenant_id"
    attribute_data_type = "String"
    mutable             = true
    required            = false
  }

  schema {
    name                = "custom:tenant_id"
    attribute_data_type = "String"
    mutable             = true
    required            = false
  }

  # Note: Identity provider and user pool client are configured separately below

  tags = merge(
    {
      Name = "${var.project_name}-user-pool"
    },
    var.enable_multitenancy ? {
      Multitenant = "true"
      BillingGroup = "bagel-multitenant"
    } : {}
  )
}

# Google Identity Provider (if configured)
# Note: Using aws_cognito_user_pool_identity_provider resource
# This will be configured separately when Google OAuth is needed
# For now, Google sign-in can be added via AWS Console if needed

# App client for MCP server
resource "aws_cognito_user_pool_client" "bagel_mcp" {
  count        = var.cognito_enabled ? 1 : 0
  name         = "${var.project_name}-mcp-client"
  user_pool_id = aws_cognito_user_pool.bagel[0].id
  generate_secret = false

  explicit_auth_flows = [
    "ALLOW_USER_PASSWORD_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_USER_SRP_AUTH"
  ]

  supported_identity_providers = var.google_client_id != "" ? ["COGNITO", "Google"] : ["COGNITO"]

  callback_urls = [
    "http://localhost:8000/callback",
    "https://your-domain.com/callback"
  ]

  logout_urls = [
    "http://localhost:8000/logout",
    "https://your-domain.com/logout"
  ]

  allowed_oauth_flows = [
    "code",
    "implicit"
  ]

  allowed_oauth_scopes = [
    "email",
    "openid",
    "profile"
  ]

  allowed_oauth_flows_user_pool_client = true
}

# Cognito Identity Pool (for AWS resource access)
resource "aws_cognito_identity_pool" "bagel" {
  count = var.cognito_enabled ? 1 : 0
  identity_pool_name               = "${var.project_name}-${var.environment}"
  allow_unauthenticated_identities = false

  cognito_identity_providers {
    client_id               = aws_cognito_user_pool_client.bagel_mcp[0].id
    provider_name           = aws_cognito_user_pool.bagel[0].endpoint
    server_side_token_check = true
  }

  # Optional: Add Google as identity provider
  dynamic "cognito_identity_providers" {
    for_each = var.google_client_id != "" ? [1] : []
    content {
      client_id               = var.google_client_id
      provider_name           = "accounts.google.com"
      server_side_token_check = false
    }
  }
}

# IAM roles for authenticated users
resource "aws_iam_role" "cognito_authenticated" {
  count = var.cognito_enabled ? 1 : 0
  name  = "${var.project_name}-cognito-authenticated"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = "cognito-identity.amazonaws.com"
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "cognito-identity.amazonaws.com:aud" = aws_cognito_identity_pool.bagel[0].id
          }
          "ForAnyValue:StringLike" = {
            "cognito-identity.amazonaws.com:amr" = "authenticated"
          }
        }
      }
    ]
  })

  # Policy to access S3 with tenant-specific prefixes
  dynamic "inline_policy" {
    for_each = var.enable_multitenancy ? [1] : []
    content {
      name = "tenant-s3-access"
      policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
          {
            Effect = "Allow"
            Action = [
              "s3:GetObject",
              "s3:PutObject",
              "s3:DeleteObject"
            ]
            Resource = [
              "${aws_s3_bucket.bagel_storage.arn}/tenants/*"
            ]
            Condition = {
              StringLike = {
                "s3:prefix" = ["tenants/${aws_cognito_identity_pool.bagel[0].id}:${"${aws_cognito_identity_pool.bagel[0].id}:*"}/*"]
              }
            }
          }
        ]
      })
    }
  }
}

resource "aws_cognito_identity_pool_roles_attachment" "bagel" {
  count            = var.cognito_enabled ? 1 : 0
  identity_pool_id = aws_cognito_identity_pool.bagel[0].id

  roles = {
    "authenticated" = aws_iam_role.cognito_authenticated[0].arn
  }
}

# Cognito Domain for Hosted UI (OAuth)
resource "aws_cognito_user_pool_domain" "bagel" {
  count        = var.cognito_enabled && var.cognito_domain != "" ? 1 : 0
  domain       = var.cognito_domain
  user_pool_id = aws_cognito_user_pool.bagel[0].id

  # Optional: Use custom domain with certificate
  # certificate_arn = var.cognito_certificate_arn
}

# Output Cognito information
output "cognito_user_pool_id" {
  description = "Cognito User Pool ID"
  value       = var.cognito_enabled ? aws_cognito_user_pool.bagel[0].id : null
}

output "cognito_user_pool_client_id" {
  description = "Cognito User Pool Client ID"
  value       = var.cognito_enabled ? aws_cognito_user_pool_client.bagel_mcp[0].id : null
}

output "cognito_domain" {
  description = "Cognito domain for hosted UI"
  value       = var.cognito_enabled && var.cognito_domain != "" ? "${var.cognito_domain}.auth.${var.aws_region}.amazoncognito.com" : null
}
