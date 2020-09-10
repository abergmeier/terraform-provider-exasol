resource "exasol_user" "user_1" {
    name = "user_1"
    password = "h12_xhz"
}

/* Only works when LDAP Server is configured
resource "exasol_user" "user_2" {
    name = "user_2"
    ldap = "cn=user_2,dc=authorization,dc=exasol,dc=com"
}
*/
