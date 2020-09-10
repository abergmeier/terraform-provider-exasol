data "local_file" "login" {
    filename = "${path.module}/.exasol_auth.json"
}
