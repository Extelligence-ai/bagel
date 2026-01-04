output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.bagel.name
}

output "ecs_service_name" {
  description = "Name of the ECS service"
  value       = aws_ecs_service.bagel_mcp.name
}

output "ecs_task_definition_arn" {
  description = "ARN of the ECS task definition"
  value       = aws_ecs_task_definition.bagel_mcp.arn
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.bagel_mcp.name
}

output "service_endpoint" {
  description = "Endpoint URL for the MCP service"
  value       = var.enable_load_balancer ? "http://${aws_lb.bagel_mcp[0].dns_name}:${var.mcp_port}" : "Use ECS task IP:${var.mcp_port}"
}

output "security_group_id" {
  description = "Security group ID for ECS tasks"
  value       = length(var.security_group_ids) > 0 ? var.security_group_ids[0] : aws_security_group.ecs_tasks[0].id
}

