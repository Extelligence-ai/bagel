#!/usr/bin/env python3
"""
Script to get tenant usage for billing purposes.

This script queries CloudWatch metrics and S3 usage to calculate
per-tenant costs for Bagel MCP Server.

Usage:
    python get-tenant-usage.py --tenant tenant-1 --start-date 2024-01-01 --end-date 2024-01-31
    python get-tenant-usage.py --all-tenants --month 2024-01
"""

import argparse
import boto3
from datetime import datetime, timedelta
from collections import defaultdict
import json

def get_s3_usage_by_tenant(s3_client, bucket_name, tenant_prefix, start_date, end_date):
    """Get S3 storage usage for a specific tenant prefix."""
    total_size = 0
    object_count = 0
    
    prefix = f"tenants/{tenant_prefix}/"
    
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                # Check if object was modified in the date range
                if start_date <= obj['LastModified'].replace(tzinfo=None) <= end_date:
                    total_size += obj['Size']
                    object_count += 1
    
    return {
        'size_bytes': total_size,
        'size_gb': total_size / (1024 ** 3),
        'object_count': object_count
    }

def get_ecs_task_usage(cloudwatch_client, cluster_name, service_name, start_time, end_time):
    """Get ECS task usage metrics."""
    metrics = {}
    
    metric_names = [
        'CPUUtilization',
        'MemoryUtilization',
        'RunningTaskCount'
    ]
    
    for metric_name in metric_names:
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/ECS',
            MetricName=metric_name,
            Dimensions=[
                {'Name': 'ClusterName', 'Value': cluster_name},
                {'Name': 'ServiceName', 'Value': service_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Average', 'Sum']
        )
        
        if response['Datapoints']:
            metrics[metric_name] = {
                'average': sum(d['Average'] for d in response['Datapoints']) / len(response['Datapoints']),
                'sum': sum(d['Sum'] for d in response['Datapoints'])
            }
    
    return metrics

def calculate_tenant_cost(s3_usage, ecs_usage, tenant_name):
    """Calculate estimated cost for a tenant."""
    costs = {}
    
    # S3 Storage Cost (Standard Storage: $0.023 per GB)
    s3_storage_cost = s3_usage['size_gb'] * 0.023
    costs['s3_storage'] = round(s3_storage_cost, 2)
    
    # S3 Requests (GET: $0.0004 per 1000, PUT: $0.005 per 1000)
    # Note: This is an estimate based on object count
    estimated_gets = s3_usage['object_count'] * 10  # Estimate 10 gets per object
    estimated_puts = s3_usage['object_count']
    s3_request_cost = (estimated_gets / 1000 * 0.0004) + (estimated_puts / 1000 * 0.005)
    costs['s3_requests'] = round(s3_request_cost, 2)
    
    # ECS Fargate Cost (proportional to usage)
    # Base: $0.04048 per vCPU-hour, $0.004445 per GB-hour
    # This is shared across tenants, so we'll calculate proportionally
    if ecs_usage.get('RunningTaskCount', {}).get('average'):
        # Estimate based on average CPU/Memory usage
        cpu_hours = ecs_usage.get('CPUUtilization', {}).get('sum', 0) / 100 * 0.5  # 0.5 vCPU
        memory_gb_hours = ecs_usage.get('MemoryUtilization', {}).get('sum', 0) / 100 * 1.0  # 1 GB
        
        # This is a simplified calculation - in reality, you'd track per-tenant
        # For now, we'll estimate based on S3 usage proportion
        costs['ecs_estimate'] = round((cpu_hours * 0.04048 + memory_gb_hours * 0.004445) * 0.1, 2)
    
    costs['total'] = round(sum(costs.values()), 2)
    
    return costs

def main():
    parser = argparse.ArgumentParser(description='Get tenant usage for billing')
    parser.add_argument('--tenant', help='Tenant name')
    parser.add_argument('--all-tenants', action='store_true', help='Get usage for all tenants')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--cluster', help='ECS cluster name')
    parser.add_argument('--service', help='ECS service name')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', choices=['json', 'table', 'csv'], default='table', help='Output format')
    
    args = parser.parse_args()
    
    s3_client = boto3.client('s3')
    cloudwatch_client = boto3.client('cloudwatch')
    
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') + timedelta(days=1)
    
    results = {}
    
    if args.all_tenants:
        # List all tenant prefixes
        paginator = s3_client.get_paginator('list_objects_v2')
        tenants = set()
        
        for page in paginator.paginate(Bucket=args.bucket, Prefix='tenants/', Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    tenant = prefix['Prefix'].replace('tenants/', '').rstrip('/')
                    tenants.add(tenant)
        
        for tenant in tenants:
            s3_usage = get_s3_usage_by_tenant(s3_client, args.bucket, tenant, start_date, end_date)
            results[tenant] = {
                's3_usage': s3_usage,
                'tenant': tenant
            }
    else:
        if not args.tenant:
            print("Error: --tenant required unless --all-tenants is used")
            return
        
        s3_usage = get_s3_usage_by_tenant(s3_client, args.bucket, args.tenant, start_date, end_date)
        results[args.tenant] = {
            's3_usage': s3_usage,
            'tenant': args.tenant
        }
    
    # Get ECS usage if cluster/service provided
    if args.cluster and args.service:
        ecs_usage = get_ecs_task_usage(
            cloudwatch_client, 
            args.cluster, 
            args.service,
            start_date,
            end_date
        )
        
        for tenant in results:
            results[tenant]['ecs_usage'] = ecs_usage
            results[tenant]['costs'] = calculate_tenant_cost(
                results[tenant]['s3_usage'],
                ecs_usage,
                tenant
            )
    
    # Output results
    if args.output == 'json':
        print(json.dumps(results, indent=2, default=str))
    elif args.output == 'csv':
        print("Tenant,Size_GB,Object_Count,S3_Storage_Cost,S3_Request_Cost,Total_Cost")
        for tenant, data in results.items():
            costs = data.get('costs', {})
            print(f"{tenant},{data['s3_usage']['size_gb']:.2f},{data['s3_usage']['object_count']},{costs.get('s3_storage', 0)},{costs.get('s3_requests', 0)},{costs.get('total', 0)}")
    else:  # table
        print(f"\nTenant Usage Report: {args.start_date} to {args.end_date}")
        print("=" * 80)
        for tenant, data in results.items():
            print(f"\nTenant: {tenant}")
            print(f"  S3 Storage: {data['s3_usage']['size_gb']:.2f} GB")
            print(f"  S3 Objects: {data['s3_usage']['object_count']}")
            if 'costs' in data:
                print(f"  Estimated Costs:")
                print(f"    S3 Storage: ${data['costs'].get('s3_storage', 0):.2f}")
                print(f"    S3 Requests: ${data['costs'].get('s3_requests', 0):.2f}")
                print(f"    Total: ${data['costs'].get('total', 0):.2f}")

if __name__ == '__main__':
    main()
