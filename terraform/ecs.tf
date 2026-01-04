# ECS Cluster
resource "aws_ecs_cluster" "bagel" {
  name = var.ecs_cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "bagel_mcp" {
  name              = "/ecs/${var.ecs_service_name}"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-mcp-logs"
  }
}

# ECS Task Definition
resource "aws_ecs_task_definition" "bagel_mcp" {
  family                   = "${var.project_name}-mcp-server"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "bagel-mcp-server"
      image     = "${aws_ecr_repository.bagel_mcp.repository_url}:latest"
      essential = true

      portMappings = [
        {
          containerPort = var.mcp_port
          protocol      = "tcp"
          hostPort      = var.mcp_port
        }
      ]

      environment = concat(
        [
          {
            name  = "BAGEL_LOCAL_HOST"
            value = "0.0.0.0"
          },
          {
            name  = "BAGEL_MCP_LOCAL_PORT"
            value = tostring(var.mcp_port)
          },
          {
            name  = "BAGEL_USE_CACHE"
            value = "true"
          },
          {
            name  = "BAGEL_CACHE_DIRECTORY"
            value = "/tmp/bagel-cache"
          },
          {
            name  = "BAGEL_STORAGE_DIRECTORY"
            value = "/tmp/bagel-storage"
          },
          {
            name  = "BAGEL_S3_BUCKET"
            value = aws_s3_bucket.bagel_storage.bucket
          },
          {
            name  = "BAGEL_MULTITENANCY_ENABLED"
            value = tostring(var.enable_multitenancy)
          }
        ],
        var.cognito_enabled ? [
          {
            name  = "COGNITO_USER_POOL_ID"
            value = aws_cognito_user_pool.bagel[0].id
          },
          {
            name  = "COGNITO_CLIENT_ID"
            value = aws_cognito_user_pool_client.bagel_mcp[0].id
          },
          {
            name  = "COGNITO_REGION"
            value = var.aws_region
          }
        ] : []
      )

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.bagel_mcp.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:${var.mcp_port}/ || nc -z localhost ${var.mcp_port} || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 40
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-mcp-task"
  }
}

# Security Group for ECS Tasks
resource "aws_security_group" "ecs_tasks" {
  count       = length(var.security_group_ids) == 0 ? 1 : 0
  name        = "${var.project_name}-ecs-tasks"
  description = "Security group for Bagel ECS tasks"
  vpc_id      = var.vpc_id

  ingress {
    description = "MCP Server"
    from_port   = var.mcp_port
    to_port     = var.mcp_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-ecs-tasks-sg"
  }
}

# ECS Service
resource "aws_ecs_service" "bagel_mcp" {
  name            = var.ecs_service_name
  cluster         = aws_ecs_cluster.bagel.id
  task_definition = aws_ecs_task_definition.bagel_mcp.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = length(var.security_group_ids) > 0 ? var.security_group_ids : [aws_security_group.ecs_tasks[0].id]
    assign_public_ip = true
  }

  # Optional: Attach to load balancer if enabled
  dynamic "load_balancer" {
    for_each = var.enable_load_balancer ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.bagel_mcp[0].arn
      container_name   = "bagel-mcp-server"
      container_port   = var.mcp_port
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_execution,
  ]

  tags = merge(
    {
      Name = "${var.project_name}-mcp-service"
      Project = var.project_name
      Environment = var.environment
    },
    var.enable_multitenancy ? {
      Multitenant = "true"
      BillingGroup = "bagel-multitenant"
    } : {
      Multitenant = "false"
    }
  )
}
