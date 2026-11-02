variable "snowflake_account" {
  type        = string
  description = "Account identifier, e.g. ORG-ACCOUNT"
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_role" {
  type    = string
  default = "SYSADMIN"
}

variable "snowflake_private_key" {
  type      = string
  sensitive = true
}

variable "database_name" {
  type    = string
  default = "DB_TRADE_DB"
}

variable "warehouses" {
  type = map(string)
  default = {
    TRADE_LOAD_WH      = "MEDIUM"
    TRADE_TRANSFORM_WH = "LARGE"
    TRADE_BI_WH        = "SMALL"
  }
}
