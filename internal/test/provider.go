package test

import (
	"errors"
	"fmt"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	CheckFailedError = errors.New("Check failed")
)

type DefaultAccProviders struct {
	Exasol    *schema.Provider
	Factories map[string]func() (*schema.Provider, error)
}

func NewDefaultAccProviders(p *schema.Provider) DefaultAccProviders {
	return DefaultAccProviders{
		Exasol: p,
		Factories: map[string]func() (*schema.Provider, error){
			"exasol": func() (*schema.Provider, error) {
				return p, nil
			},
		},
	}
}

type ObjectTest struct {
	ResourceName string
	DbName       string
	Stmt         string
	Config       string
}

func HCLProviderFromConf(conf exasol.ConnConf) string {
	return fmt.Sprintf(`provider "exasol" {
		ip       = "%s"
		username = "%s"
		password = "%s"
	}`, conf.Host, conf.Username, conf.Password)
}

func False(p *schema.Provider, cb func(binding.Conn) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := p.Meta().(*exaprovider.Client)
		conn := OpenManualConnection(c)
		defer conn.Close()

		t, err := cb(conn.Conn)
		if err != nil {
			return err
		}

		if t {
			return CheckFailedError
		}

		return nil
	}
}

func True(p *schema.Provider, cb func(binding.Conn) (bool, error)) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := p.Meta().(*exaprovider.Client)
		conn := OpenManualConnection(c)
		defer conn.Close()

		t, err := cb(conn.Conn)
		if err != nil {
			return err
		}

		if !t {
			return CheckFailedError
		}

		return nil
	}
}
