provider "exasol" {
  username = jsondecode(google_secret_manager_secret_version.login.secret_data).username
  password = jsondecode(google_secret_manager_secret_version.login.secret_data).password
}
