resource "exa_connection" "dummy" {
    name     = "singleinstance"
    to       = "192.168.1.1"
    username = "sys"
    password = "exasol"
}
