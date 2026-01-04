# Data source to automatically get AWS account ID
data "aws_caller_identity" "current" {}

# Data source to get current AWS region
data "aws_region" "current" {}

# Output AWS account information
output "aws_account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS Region"
  value       = data.aws_region.current.name
}

output "aws_user_arn" {
  description = "ARN of the current AWS user"
  value       = data.aws_caller_identity.current.arn
}
