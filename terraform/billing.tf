# Billing and Usage Tracking for Multitenancy
# This enables cost allocation and usage tracking per tenant

# Cost Allocation Tags
# These tags are automatically picked up by AWS Cost Explorer for billing

# Tag the S3 bucket with tenant-aware structure
locals {
  # Create a map of tenant tags for cost allocation
  tenant_tags = var.enable_multitenancy ? {
    for tenant in var.tenant_names : "tenant-${tenant}" => {
      "Tenant" = tenant
      "CostCenter" = "bagel-${tenant}"
      "BillingGroup" = "bagel-multitenant"
    }
  } : {}
  
  # Common tags for all resources
  common_tags = merge(
    {
      "Project"     = var.project_name
      "Environment" = var.environment
      "ManagedBy"   = "terraform"
    },
    var.enable_multitenancy ? {
      "Multitenant" = "true"
    } : {
      "Multitenant" = "false"
    }
  )
}

# CloudWatch Metrics for Tenant Usage Tracking
resource "aws_cloudwatch_metric_alarm" "tenant_s3_usage" {
  for_each = var.enable_multitenancy ? toset(var.tenant_names) : toset([])
  
  alarm_name          = "${var.project_name}-tenant-${each.key}-s3-usage"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "BucketSizeBytes"
  namespace           = "AWS/S3"
  period              = 86400  # 24 hours
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Tracks S3 storage usage for tenant ${each.key}"
  treat_missing_data  = "notBreaching"

  dimensions = {
    BucketName = aws_s3_bucket.bagel_storage.bucket
    StorageType = "StandardStorage"
  }

  tags = merge(
    local.common_tags,
    {
      "Tenant" = each.key
      "MetricType" = "billing"
    }
  )
}

# CloudWatch Log Group for Tenant Usage Logs
resource "aws_cloudwatch_log_group" "tenant_usage" {
  count             = var.enable_multitenancy ? 1 : 0
  name              = "/aws/bagel/tenant-usage"
  retention_in_days = 90

  tags = merge(
    local.common_tags,
    {
      "Purpose" = "tenant-billing"
    }
  )
}

# S3 Inventory Configuration for Tenant Usage Tracking
resource "aws_s3_bucket_inventory" "tenant_usage" {
  count  = var.enable_multitenancy ? 1 : 0
  bucket = aws_s3_bucket.bagel_storage.id
  name   = "tenant-usage-inventory"

  included_object_versions = "All"

  schedule {
    frequency = "Daily"
  }

  destination {
    bucket {
      bucket_arn = aws_s3_bucket.bagel_storage.arn
      prefix     = "inventory/tenant-usage/"
      format     = "CSV"
    }
  }

  optional_fields = [
    "Size",
    "LastModifiedDate",
    "StorageClass",
    "ETag",
    "IntelligentTieringAccessTier"
  ]
}

# IAM Policy for ECS tasks to write usage metrics
resource "aws_iam_role_policy" "ecs_task_cloudwatch_metrics" {
  count = var.enable_multitenancy ? 1 : 0
  name  = "${var.project_name}-ecs-task-cloudwatch-metrics"
  role  = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "Bagel/TenantUsage"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.tenant_usage[0].arn}:*"
      }
    ]
  })
}

# Cost Allocation Tags for ECS Service
# Tags are applied directly to the main service in ecs.tf
# This ensures AWS Cost Explorer can track costs per tenant

# Output billing information
output "billing_enabled" {
  description = "Whether billing/usage tracking is enabled"
  value       = var.enable_multitenancy
}

output "cost_allocation_tags" {
  description = "Tags used for cost allocation"
  value = var.enable_multitenancy ? {
    for tenant in var.tenant_names : tenant => {
      "Tenant"       = tenant
      "CostCenter"   = "bagel-${tenant}"
      "BillingGroup" = "bagel-multitenant"
    }
  } : {}
}

output "usage_log_group" {
  description = "CloudWatch log group for tenant usage tracking"
  value       = var.enable_multitenancy ? aws_cloudwatch_log_group.tenant_usage[0].name : null
}

output "s3_inventory_enabled" {
  description = "Whether S3 inventory is enabled for usage tracking"
  value       = var.enable_multitenancy
}
