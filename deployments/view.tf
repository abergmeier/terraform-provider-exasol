// See https://docs.exasol.com/sql/create_view.html

resource "exasol_view" "my_view" {
  name     = "my_view"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "select x from t"
}

resource "exasol_table" "my_view2" {
  name     = "my_view2"
  schema   = exasol_physical_schema.my_schema.name
  subquery = "select y from t"
  replace  = true
}

resource "exasol_table" "my_view3" {
  name     = "my_view3"
  schema   = exasol_physical_schema.my_schema.name
  columns  = [
    "col_1",
  ]
  subquery = "select max(y) from t"
  replace  = true
}
