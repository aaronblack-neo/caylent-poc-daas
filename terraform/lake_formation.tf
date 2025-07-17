resource "aws_lakeformation_permissions" "full_access_to_db" {
  count       = length(local.databases_with_roles)
  permissions = ["ALL"]
  principal   = local.databases_with_roles[count.index].role

  database {
    name = local.databases_with_roles[count.index].database
  }
}

resource "aws_lakeformation_permissions" "full_access_to_tables" {
  count       = length(local.databases_with_roles)
  permissions = ["ALL"]
  principal   = local.databases_with_roles[count.index].role

  table {
    database_name = local.databases_with_roles[count.index].database
    wildcard      = true
  }
}
