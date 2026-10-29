# Starter IaC: validate provider schema/version and least-privilege setup before apply.
provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
}
resource "snowflake_database" "trade_db" {
  name                        = var.database_name
  comment                     = "Trade processing case study"
  data_retention_time_in_days = 1
}
resource "snowflake_warehouse" "trade_wh" {
  name           = var.warehouse_name
  warehouse_size = var.warehouse_size
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Trade pipeline warehouse"
}
resource "snowflake_schema" "raw" {
  database = snowflake_database.trade_db.name
  name     = "RAW"
}
resource "snowflake_schema" "curated" {
  database = snowflake_database.trade_db.name
  name     = "CURATED"
}
resource "snowflake_schema" "audit" {
  database = snowflake_database.trade_db.name
  name     = "AUDIT"
}
