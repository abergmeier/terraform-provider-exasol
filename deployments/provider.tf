provider "exasol" {
  username = "sys"
  password = "exasol"
}

terraform {
  required_providers {
    exasol = {
      source  = "abergmeier/exasol"
    }
  }
}
