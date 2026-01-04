# Application Load Balancer (optional)
resource "aws_lb" "bagel_mcp" {
  count              = var.enable_load_balancer ? 1 : 0
  name               = "${var.project_name}-mcp-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb[0].id]
  subnets            = var.subnet_ids

  enable_deletion_protection = false
  enable_http2               = true
  enable_cross_zone_load_balancing = true

  tags = {
    Name = "${var.project_name}-mcp-alb"
  }
}

# Security Group for ALB
resource "aws_security_group" "alb" {
  count       = var.enable_load_balancer ? 1 : 0
  name        = "${var.project_name}-alb-sg"
  description = "Security group for Bagel ALB"
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
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
    Name = "${var.project_name}-alb-sg"
  }
}

# Target Group for ECS Tasks
resource "aws_lb_target_group" "bagel_mcp" {
  count                = var.enable_load_balancer ? 1 : 0
  name                 = "${var.project_name}-mcp-tg"
  port                 = var.mcp_port
  protocol             = "HTTP"
  vpc_id               = var.vpc_id
  target_type          = "ip"
  deregistration_delay = 30

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    path                = "/"
    protocol            = "HTTP"
    matcher             = "200"
  }

  tags = {
    Name = "${var.project_name}-mcp-tg"
  }
}

# ALB Listener
resource "aws_lb_listener" "bagel_mcp" {
  count             = var.enable_load_balancer ? 1 : 0
  load_balancer_arn = aws_lb.bagel_mcp[0].arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.bagel_mcp[0].arn
  }
}

# Output ALB DNS name
output "alb_dns_name" {
  description = "DNS name of the Application Load Balancer"
  value       = var.enable_load_balancer ? aws_lb.bagel_mcp[0].dns_name : null
}
