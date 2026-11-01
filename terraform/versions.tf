terraform {
  required_version = ">= 1.6.0"
  required_providers {
    snowflake = { source = "Snowflake-Labs/snowflake"; version = ">= 0.90.0" }
  }
}
provider "snowflake" {
  account = var.snowflake_account
  user = var.snowflake_user
  role = var.snowflake_role
  authenticator = "SNOWFLAKE_JWT"
  private_key = var.snowflake_private_key
}
