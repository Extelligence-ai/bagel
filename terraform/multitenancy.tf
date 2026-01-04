# Multitenancy Support for Bagel MCP Server
# This file provides options for multi-tenant deployments

# Option 1: Tenant-specific S3 prefixes (recommended for shared service)
# Each tenant gets their own prefix in the S3 bucket
# Example: s3://bucket/tenant-1/robologs/, s3://bucket/tenant-2/robologs/

# Option 2: Separate ECS services per tenant (recommended for isolation)
# Use this if you need complete isolation between tenants

# Variable to enable multitenancy
variable "enable_multitenancy" {
  description = "Enable multitenancy support"
  type        = bool
  default     = false
}

variable "tenant_names" {
  description = "List of tenant names (only used if enable_multitenancy is true)"
  type        = list(string)
  default     = []
}

# Tenant-specific S3 bucket prefixes (if using shared service)
# This creates IAM policies that restrict access to tenant-specific prefixes
resource "aws_iam_role_policy" "ecs_task_s3_tenant" {
  for_each = var.enable_multitenancy ? toset(var.tenant_names) : toset([])
  name     = "${var.project_name}-ecs-task-s3-tenant-${each.key}"
  role     = aws_iam_role.ecs_task.id

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
        Resource = "${aws_s3_bucket.bagel_storage.arn}/tenants/${each.key}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.bagel_storage.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["tenants/${each.key}/*"]
          }
        }
      }
    ]
  })
}

# Optional: Separate ECS services per tenant (for complete isolation)
# Uncomment and customize if you need separate services per tenant
/*
resource "aws_ecs_service" "bagel_mcp_tenant" {
  for_each        = var.enable_multitenancy ? toset(var.tenant_names) : toset([])
  name            = "${var.ecs_service_name}-${each.key}"
  cluster         = aws_ecs_cluster.bagel.id
  task_definition = aws_ecs_task_definition.bagel_mcp.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = length(var.security_group_ids) > 0 ? var.security_group_ids : [aws_security_group.ecs_tasks[0].id]
    assign_public_ip = true
  }

  tags = {
    Name      = "${var.project_name}-mcp-service-${each.key}"
    Tenant    = each.key
  }
}
*/

# Output tenant information
output "multitenancy_enabled" {
  description = "Whether multitenancy is enabled"
  value       = var.enable_multitenancy
}

output "tenant_names" {
  description = "List of configured tenants"
  value       = var.enable_multitenancy ? var.tenant_names : []
}
