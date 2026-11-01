# Terraform starter

Use the Snowflake Terraform provider to provision databases, schemas, warehouses, roles, grants, stages, pipes, tasks and notification integrations. Provider resource coverage and syntax vary by provider version; pin the provider and validate with `terraform init -backend=false`, `terraform fmt -check`, and `terraform validate`.

Cloud bucket, event notification and IAM/SNS/SQS/Event Grid/PubSub resources should live in a cloud-specific module. Do not store private keys or passwords in Terraform state; use a secret manager and key-pair authentication. This starter intentionally does not create cloud resources without knowing the target cloud/account.
