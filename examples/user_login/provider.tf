provider "exasol" {
  username = jsondecode(data.local_file.login.content).username
  password = jsondecode(data.local_file.login.content).password
}
