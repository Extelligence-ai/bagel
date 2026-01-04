# Moving Deployment Files to matcha-cloud Repository

## Files to Move

### 1. Terraform Configuration (entire directory)
- `terraform/` - All Terraform files for ECS deployment
  - All `.tf` files (main.tf, variables.tf, ecs.tf, etc.)
  - All `.md` documentation files
  - All `.sh` deployment scripts
  - `terraform.tfvars` (may need to be updated for new repo)
  - `terraform.tfvars.example`
  - `scripts/` directory

### 2. Dockerfile for ECS
- `docker/Dockerfile.ecs` - ECS-specific Dockerfile

### 3. Old ECS Directory (optional - may be superseded by Terraform)
- `ecs/` - Original ECS deployment files (may not be needed if using Terraform)

## Proposed Structure in matcha-cloud

```
matcha-cloud/
├── README.md
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── ecs.tf
│   ├── ecr.tf
│   ├── iam.tf
│   ├── s3.tf
│   ├── load_balancer.tf
│   ├── autoscaling.tf
│   ├── cognito.tf
│   ├── multitenancy.tf
│   ├── billing.tf
│   ├── data.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   ├── deploy-complete.sh
│   ├── deploy-and-get-url.sh
│   ├── find-vpc-info.sh
│   ├── scripts/
│   │   ├── get-tenant-usage.py
│   │   ├── onboard-user.sh
│   │   └── verify-tenant.sh
│   └── *.md (all documentation)
├── docker/
│   └── Dockerfile.ecs
└── .gitignore
```

## Changes Needed

1. **Update Dockerfile.ecs**: Change paths to reference bagel repo (or clone it)
2. **Update deployment scripts**: May need to adjust paths
3. **Update Terraform variables**: May need to reference bagel image differently
4. **Create README**: Explain that this deploys bagel from another repo

## Questions

1. Does `matcha-cloud` repo already exist?
2. Should we:
   - **Move** files (delete from bagel repo)?
   - **Copy** files (keep in both repos)?
3. How should matcha-cloud reference bagel?
   - Clone bagel repo during build?
   - Reference bagel as a dependency?
   - Build from a published Docker image?
4. Should we keep any deployment files in bagel repo, or move everything?

