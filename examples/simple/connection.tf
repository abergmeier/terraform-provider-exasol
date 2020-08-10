resource "exa_connection" "dummy" {
    name = "dummy"
    to {
        ftp = 192.168.1.1
    }
    user = ""
    identified_by = ""
}
