// See https://docs.exasol.com/sql/create_connection.htm

resource "exasol_connection" "ftp_connection" {
  name     = "ftp_connection"
  to       = "ftp://192.168.1.1/"
  username = "agent_007"
  password = "secret"
}

resource "exasol_connection" "exa_connection" {
  name = "exa_connection"
  to   = "192.168.6.11..14:8563"
}

resource "exasol_connection" "ora_connection" {
  name = "ora_connection"
  to   = <<EOF
    (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.6.54)(PORT = 1521))
    (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = orcl)))
EOF
}

resource "exasol_connection" "jdbc_connection_1" {
  name = "jdbc_connection_1"
  to   = "jdbc:mysql://192.168.6.1/my_db"
}

resource "exasol_connection" "jdbc_connection_2" {
  name = "jdbc_connection_2"
  to   = "jdbc:postgresql://192.168.6.2:5432/my_db?stringtype=unspecified"
}
