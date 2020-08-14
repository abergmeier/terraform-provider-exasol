resource "exa_connection" "dummy" {
    name     = "dummy"
    to       = "192.168.1.1"
    username = "foo"
    password = "bar"
}
