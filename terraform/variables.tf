variable "database_name" { type = string, default = "TRADE_DB" }
variable "warehouse_name" { type = string, default = "TRADE_PIPELINE_WH" }
variable "warehouse_size" { type = string, default = "XSMALL" }
variable "snowflake_organization" { type = string }
variable "snowflake_account" { type = string }
variable "snowflake_user" { type = string }
variable "snowflake_password" { type = string, sensitive = true }
variable "snowflake_role" { type = string, default = "ACCOUNTADMIN" }
