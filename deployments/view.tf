// See https://docs.exasol.com/sql/create_view.html

resource "exasol_view" "my_view" {
  name     = "my_view"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "select a from ${exasol_table.t1.schema}.${exasol_table.t1.name}"
}

resource "exasol_view" "my_view2" {
  name     = "my_view2"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "select b from ${exasol_table.t1.schema}.${exasol_table.t1.name}"
  replace  = true
}

resource "exasol_view" "my_view3" {
  name     = "my_view3"
  schema   = exasol_physical_schema.my_schema.name
  column {
    name    = "col_1"
    comment = "Our first column"
  }
  column {
    name    = "col_2"
    comment = "Our second column"
  }
  subquery = "select max(b), '1' from ${exasol_table.t1.schema}.${exasol_table.t1.name}"
  replace  = true
}
