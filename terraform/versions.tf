terraform {
  required_version = ">= 1.6.0"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 0.95.0, < 1.0.0"
    }
  }
}
