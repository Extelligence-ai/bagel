# Auto Scaling Target
resource "aws_appautoscaling_target" "bagel_mcp" {
  count              = var.enable_auto_scaling ? 1 : 0
  max_capacity       = var.max_capacity
  min_capacity       = var.min_capacity
  resource_id        = "service/${aws_ecs_cluster.bagel.name}/${aws_ecs_service.bagel_mcp.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

# Auto Scaling Policy - CPU
resource "aws_appautoscaling_policy" "bagel_mcp_cpu" {
  count              = var.enable_auto_scaling ? 1 : 0
  name               = "${var.project_name}-mcp-cpu-autoscaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.bagel_mcp[0].resource_id
  scalable_dimension = aws_appautoscaling_target.bagel_mcp[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.bagel_mcp[0].service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = var.target_cpu_utilization
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}

# Auto Scaling Policy - Memory
resource "aws_appautoscaling_policy" "bagel_mcp_memory" {
  count              = var.enable_auto_scaling ? 1 : 0
  name               = "${var.project_name}-mcp-memory-autoscaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.bagel_mcp[0].resource_id
  scalable_dimension = aws_appautoscaling_target.bagel_mcp[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.bagel_mcp[0].service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageMemoryUtilization"
    }
    target_value       = var.target_memory_utilization
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}
