package test

import (
	"fmt"

	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/internal/resourceprovider"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

var (
	// DefaultAccProviders are all Providers AKA exasol
	DefaultAccProviders map[string]terraform.ResourceProvider
)

type ObjectTest struct {
	ResourceName string
	DbName       string
	Stmt         string
	Config       string
}

func init() {
	testAccProvider := resourceprovider.Provider().(*schema.Provider)
	DefaultAccProviders = map[string]terraform.ResourceProvider{
		"exasol": testAccProvider,
	}
}

func ProviderInHCL(locked *exaprovider.Locked) string {
	return fmt.Sprintf(`provider "exasol" {
	ip       = "%s"
	username = "%s"
	password = "%s"
}`, locked.Conn.Conf.Host, locked.Conn.Conf.Username, locked.Conn.Conf.Password)
}
