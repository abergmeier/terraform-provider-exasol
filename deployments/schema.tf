
// See examples from https://docs.exasol.com/sql/create_schema.htm

resource "exasol_physical_schema" "my_schema" {
  name = "my_schema"
}

/*
resource "exasol_virtual_schema" "hive" {
  name = "hive"
  adapter_script = "adapter.jdbc_adapter"
  properties = {
    SQL_DIALECT       = "HIVE"
    CONNECTION_STRING = "jdbc:hive2://localhost:10000/default"
    SCHEMA_NAME	      = "default"
    USERNAME	      = "hive-usr"
    PASSWORD	      = "hive-pwd"
  }
*/
