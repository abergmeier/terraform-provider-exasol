package test

import (
	"errors"
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/resourceprovider"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	AccProvider      *schema.Provider
	CheckFailedError = errors.New("Check failed")
	// DefaultAccProviders are all Providers AKA exasol
	DefaultAccProviders map[string]*schema.Provider
)

func init() {
	AccProvider = resourceprovider.Provider()
}

type ObjectTest struct {
	ResourceName string
	DbName       string
	Stmt         string
	Config       string
}

func init() {
	DefaultAccProviders = map[string]*schema.Provider{
		"exasol": AccProvider,
	}
}

func HCLProviderFromConf(conf exasol.ConnConf) string {
	return fmt.Sprintf(`provider "exasol" {
		ip       = "%s"
		username = "%s"
		password = "%s"
	}`, conf.Host, conf.Username, conf.Password)
}

func False(cb func(internal.Conn) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := AccProvider.Meta().(*exaprovider.Client)
		locked := c.Lock()
		defer locked.Unlock()

		t, err := cb(locked.Conn)
		if err != nil {
			return err
		}

		if t {
			return CheckFailedError
		}

		return nil
	}
}

func True(cb func(internal.Conn) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := AccProvider.Meta().(*exaprovider.Client)
		locked := c.Lock()
		defer locked.Unlock()

		t, err := cb(locked.Conn)
		if err != nil {
			return err
		}

		if !t {
			return CheckFailedError
		}

		return nil
	}
}
