locals {
  databases = ["raw"]
  roles     = [aws_iam_role.glue_etl_role.arn, "arn:aws:iam::664418979226:user/marcos.foglino@caylent.com", "arn:aws:iam::664418979226:user/bruno.souza@caylent.com"]
  databases_with_roles = flatten([
    for role in local.roles : [
      #for database in concat(var.databases_name, ["default"]) : {
      for database in local.databases : {
        role     = role
        database = database
      }
    ]
  ])
}

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
