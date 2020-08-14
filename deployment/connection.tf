resource "exasol_connection" "dummy" {
  name     = "test"
  to       = "192.168.1.1"
  username = "foo"
  password = "bar"
}
