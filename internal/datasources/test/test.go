package test

import (
	"fmt"

	"github.com/abergmeier/terraform-exasol/internal/resourceprovider"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var (
	// DefaultAccProviders are all Providers AKA exasol
	DefaultAccProviders map[string]*schema.Provider
)

type ObjectTest struct {
	ResourceName string
	DbName       string
	Stmt         string
	Config       string
}

func init() {
	testAccProvider := resourceprovider.Provider()
	DefaultAccProviders = map[string]*schema.Provider{
		"exasol": testAccProvider,
	}
}

func HCLProviderFromConf(conf *exasol.ConnConf) string {
	return fmt.Sprintf(`provider "exasol" {
		ip       = "%s"
		username = "%s"
		password = "%s"
	}`, conf.Host, conf.Username, conf.Password)
}
