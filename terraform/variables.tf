variable "snowflake_account" { type = string }
variable "snowflake_user" { type = string }
variable "snowflake_role" { type = string; default = "SYSADMIN" }
variable "snowflake_private_key" { type = string; sensitive = true }
