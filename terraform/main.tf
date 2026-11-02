# Starter IaC. Pin/validate the provider version; roles and grants live in sql/60_grants.sql.
provider "snowflake" {
  account       = var.snowflake_account
  user          = var.snowflake_user
  role          = var.snowflake_role
  authenticator = "JWT"
  private_key   = var.snowflake_private_key
}

resource "snowflake_database" "trade_db" {
  name                        = var.database_name
  comment                     = "Trade processing case study"
  data_retention_time_in_days = 1
}

resource "snowflake_schema" "schemas" {
  for_each = toset(["RAW", "STANDARD", "CURATED", "AUDIT", "OPS"])
  database = snowflake_database.trade_db.name
  name     = each.key
}

resource "snowflake_warehouse" "wh" {
  for_each       = var.warehouses
  name           = each.key
  warehouse_size = each.value
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Trade pipeline workload warehouse"
}
