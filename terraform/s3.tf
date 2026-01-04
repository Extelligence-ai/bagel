# S3 Bucket for Bagel storage (robologs, artifacts, datasets)
resource "aws_s3_bucket" "bagel_storage" {
  bucket = "${var.project_name}-${var.environment}-storage-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    {
      Name        = "${var.project_name}-storage"
      Environment = var.environment
      Project     = var.project_name
      ManagedBy   = "terraform"
    },
    var.enable_multitenancy ? {
      Multitenant = "true"
      BillingGroup = "bagel-multitenant"
    } : {
      Multitenant = "false"
    }
  )
}

# Enable versioning for data protection
resource "aws_s3_bucket_versioning" "bagel_storage" {
  bucket = aws_s3_bucket.bagel_storage.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "bagel_storage" {
  bucket = aws_s3_bucket.bagel_storage.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle policy to manage old versions
resource "aws_s3_bucket_lifecycle_configuration" "bagel_storage" {
  bucket = aws_s3_bucket.bagel_storage.id

  rule {
    id     = "delete-old-versions"
    status = "Enabled"
    
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "transition-to-ia"
    status = "Enabled"
    
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "transition-to-glacier"
    status = "Enabled"
    
    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "bagel_storage" {
  bucket = aws_s3_bucket.bagel_storage.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets  = true
}

# IAM policy for ECS tasks to access S3
resource "aws_iam_role_policy" "ecs_task_s3" {
  name = "${var.project_name}-ecs-task-s3"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bagel_storage.arn,
          "${aws_s3_bucket.bagel_storage.arn}/*"
        ]
      }
    ]
  })
}

# Output S3 bucket name
output "s3_bucket_name" {
  description = "Name of the S3 bucket for Bagel storage"
  value       = aws_s3_bucket.bagel_storage.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.bagel_storage.arn
}
