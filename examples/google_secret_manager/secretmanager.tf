
/*
 * Secret might look like:
 * {"username"="sys","password"="exasol"}
*/
data "google_secret_manager_secret_version" "login" {
  secret = "exasol-login"
}
